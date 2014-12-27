namespace Nessos.MBrace.SampleRuntime.Actors

open System
open System.Collections.Generic

open Nessos.Thespian

open Nessos.Vagrant
open Nessos.MBrace.Runtime.Vagrant
open Nessos.MBrace.SampleRuntime.Tasks

type internal TaskInfo = Task * AssemblyId list * int * LeaseMonitor

type private WorkerMsg =
    | RunTask of task: Task * dependencies: AssemblyId list * faultCount: int * leaseMonitor: LeaseMonitor


type internal WorkerRef private (source: ActorRef<WorkerMsg>) =
    static member Init() = ()

    member __.SourceId = source.Id

    member __.RunTask(task: Task, dependencies: AssemblyId list, faultCount: int, leaseMonitor: LeaseMonitor) =
        source <-!- RunTask(task, dependencies, faultCount, leaseMonitor)

type private SchedullerMsg =
    | ScheduleTask of task: Task * dependencies: AssemblyId list * faultCount: int * leaseMonitor: LeaseMonitor
    | AddWorkers of WorkerRef list
    | RemoveWorkers of WorkerRef list

type private SchedullerState =
    {
        WorkerQueue: Queue<WorkerRef>
        UnschedulledTasks: TaskInfo list
    }

type internal Scheduler private (source: ActorRef<SchedullerMsg>) =
    static let behavior (state: SchedullerState ) (msg: SchedullerMsg) =
        let scheduleTask task dependencies faultCount leaseMonitor =
            async {
                let worker = state.WorkerQueue.Dequeue()
                do! worker.RunTask(task, dependencies, faultCount, leaseMonitor)
                state.WorkerQueue.Enqueue worker
            }
            
        async {
            match msg with
            | AddWorkers workers ->
                for worker in workers do state.WorkerQueue.Enqueue worker
                for (task, dependencies, faultCount, leaseMonitor) in state.UnschedulledTasks do
                    do! scheduleTask task dependencies faultCount leaseMonitor
                return state
            | RemoveWorkers workers ->
               let removeSet = workers |> Seq.map (fun workerRef -> workerRef.SourceId) |> Set.ofSeq
               let newWorkers =  state.WorkerQueue |> Seq.filter (fun w -> removeSet |> Set.contains w.SourceId |> not)

               return { state with WorkerQueue = new Queue<WorkerRef>(newWorkers) }
            | ScheduleTask(task, dependencies, faultCount, leaseMonitor) ->
                if state.WorkerQueue.Count = 0 then
                    return { state with UnschedulledTasks = (task, dependencies, faultCount, leaseMonitor)::state.UnschedulledTasks }
                else
                    do! scheduleTask task dependencies faultCount leaseMonitor
                    return state
        }

    static member Init(initWorkers: WorkerRef list) =
        let initState = { WorkerQueue = new Queue<WorkerRef>(initWorkers); UnschedulledTasks = [] }
        let source =
            Actor.Stateful initState behavior
            |> Actor.Publish
            |> Actor.ref

        new Scheduler(source)

    member __.ScheduleTask(task: Task, dependencies: AssemblyId list, faultCount: int, leaseMonitor: LeaseMonitor) =
        source <-!- ScheduleTask(task, dependencies, faultCount, leaseMonitor)

    member __.AddWorkers(workers: WorkerRef list) = source <-!- AddWorkers(workers)
    member __.RemoveWorkers(workers: WorkerRef list) = source <-!- RemoveWorkers(workers)

