﻿namespace MBrace.Runtime

open System
open System.Collections.Concurrent
open System.Runtime.Serialization

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.PrettyPrinters

/// A Serializable object used to identify a specific worker in a cluster
/// Can be used to point computations for execution at specific machines
[<Sealed; DataContract>]
type WorkerRef private (runtime : IRuntimeManager, workerId : IWorkerId) =
    
    [<DataMember(Name = "RuntimeId")>]
    let runtimeId = runtime.Id
    [<DataMember(Name = "WorkerId")>]
    let workerId = workerId

    let initWorkerStateReader () =
        let rm = RuntimeManagerRegistry.Resolve runtimeId
        let getId = async { return! rm.WorkerManager.TryGetWorkerState workerId }
        CacheAtom.Create(getId, intervalMilliseconds = 100, keepLastResultOnError = true)

    [<IgnoreDataMember>]
    let mutable cvalue = initWorkerStateReader()
    let getState () =
        match cvalue.Value with
        | None -> invalidOp <| sprintf "Worker '%s' is no longer part of runtime '%s'." workerId.Id runtimeId.Id
        | Some cv -> cv

    [<OnDeserialized>]
    member private __.OnDeserialized (_ : StreamingContext) =
        cvalue <- initWorkerStateReader()

    /// Runtime identifier
    member __.RuntimeId = runtimeId
    /// Worker identifier
    member __.WorkerId = workerId

    /// Gets the worker hostname
    member __.Hostname = getState().Info.Hostname
    /// Worker identifier
    member __.Id = workerId.Id
    /// Gets the total cpu usage percentage of the worker host
    member __.CpuUsage = getState().PerformanceMetrics.CpuUsage
    /// Gets the total processor count of the worker host
    member __.ProcessorCount = getState().Info.ProcessorCount
    /// Gets the OS identifier of the worker process
    member __.ProcessId = getState().Info.ProcessId
    /// Gets the Max Cpu clock speed in MHz
    member __.MaxCpuClock = getState().PerformanceMetrics.MaxClockSpeed
    /// Gets the total memory usage of the worker host in MB
    member __.MemoryUsage = getState().PerformanceMetrics.MemoryUsage
    /// Gets the total memory capacity of the worker host in MB
    member __.TotalMemory = getState().PerformanceMetrics.TotalMemory
    /// Gets the number of cloud jobs that are active in the current worker
    member __.ActiveJobs = getState().CurrentJobCount
    /// Gets the maximum job count permitted as set by worker configuration
    member __.MaxJobCount = getState().Info.MaxJobCount
    /// Gets the network upload usage in KB/s
    member __.NetworkUsageUp = getState().PerformanceMetrics.NetworkUsageUp
    /// Gets the network download usage in KB/s
    member __.NetworkUsageDown = getState().PerformanceMetrics.NetworkUsageDown
    /// Gets the latest time that a worker heartbeat was received
    member __.LastHeartbeat = getState().LastHeartbeat
    /// Gets the initialization/subscription time of the worker process
    member __.InitializationTime = getState().InitializationTime
    /// Gets the worker execution status
    member __.Status = getState().ExecutionStatus

    override __.Equals(other:obj) =
        match other with
        | :? WorkerRef as w -> areReflectiveEqual runtimeId w.RuntimeId && areReflectiveEqual workerId w.WorkerId
        | _ -> false

    override __.GetHashCode() = hash2 runtimeId workerId

    override __.ToString() = workerId.ToString()

    interface IWorkerRef with
        member x.CompareTo(obj: obj): int = 
            match obj with
            | :? WorkerRef as w -> compare2 runtimeId workerId w.RuntimeId w.WorkerId
            | _ -> invalidArg "obj" "invalid comparand."
        
        member x.Hostname: string = 
            getState().Info.Hostname
        
        member x.Id: string = 
            workerId.Id
        
        member x.ProcessId: int = 
            getState().Info.ProcessId
        
        member x.ProcessorCount: int = 
            getState().Info.ProcessorCount

        member x.MaxCpuClock = 
            match x.MaxCpuClock with
            | c when c.HasValue -> c.Value
            | _ -> invalidOp "Could not get CPU clock speed for worker."
        
        member x.Type: string = 
           runtimeId.Id
        

    /// Gets a printed report on worker status
    member w.GetInfo() : string = WorkerReporter.Report([|w|], "Worker", borders = false)
    /// Prints a report on worker to stdout
    member w.ShowInfo () : unit = Console.WriteLine(w.GetInfo())

    /// <summary>
    ///     Creates a new WorkerRef instance for given runtime and worker id.
    /// </summary>
    /// <param name="runtime">Runtime management object.</param>
    /// <param name="workerId">Worker identifier.</param>
    static member Create(runtime : IRuntimeManager, workerId : IWorkerId) =
        new WorkerRef(runtime, workerId)


/// WorkerRef information reporting
and internal WorkerReporter private () =
    
    static let template : Field<WorkerRef> list = 
        let inline ( *?) x (y : Nullable<_>) =
            if y.HasValue then new Nullable<_>(x * y.Value)
            else new Nullable<_>()

        let inline (?/?) (x : Nullable<_>) (y : Nullable<_>) =
            if x.HasValue && y.HasValue then new Nullable<_>(x.Value / y.Value)
            else new Nullable<_>()

        let double_printer (value : Nullable<double>) = 
            if value.HasValue then sprintf "%.1f" value.Value
            else "N/A"

        [ Field.create "Id" Left (fun w -> w.Id)
          Field.create "Status" Left (fun p -> string p.Status)
          Field.create "% CPU / Cores" Center (fun p -> sprintf "%s / %d" (double_printer p.CpuUsage) p.ProcessorCount)
          Field.create "CPU Clock" Left (fun p -> sprintf "%s MHz" (double_printer p.MaxCpuClock))
          Field.create "% Memory / Total(MB)" Center (fun p ->
                let memPerc = 100. *? p.MemoryUsage ?/? p.TotalMemory |> double_printer
                sprintf "%s / %s" memPerc <| double_printer p.TotalMemory
            )
          Field.create "Network(ul/dl : KB/s)" Center (fun n -> sprintf "%s / %s" <| double_printer n.NetworkUsageUp <| double_printer n.NetworkUsageDown)
          Field.create "Jobs" Center (fun p -> sprintf "%d / %d" p.ActiveJobs p.MaxJobCount)
          Field.create "Hostname" Left (fun p -> p.Hostname)
          Field.create "Process Id" Right (fun p -> p.ProcessId)
          Field.create "Heartbeat" Left (fun p -> p.LastHeartbeat)
          Field.create "Initialization Time" Left (fun p -> p.InitializationTime) 
        ]
    
    /// <summary>
    ///     Generates a string displaying information on provided workers.
    /// </summary>
    /// <param name="workers">Workers to be displayed.</param>
    /// <param name="title">Title for display.</param>
    /// <param name="borders">Use borders for displayed information.</param>
    static member Report(workers : seq<WorkerRef>, title : string, borders : bool) : string = 
        let ws = workers
                 |> Seq.sortBy (fun w -> w.InitializationTime)
                 |> Seq.toList

        Record.PrettyPrint(template, ws, title, borders)