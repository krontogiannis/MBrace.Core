﻿namespace MBrace.Runtime

open System
open System.Diagnostics

open MBrace.Core
open MBrace.Core.Internals

open Nessos.Vagabond
open Nessos.Vagabond.AppDomainPool

open MBrace.Runtime.Utils

/// Job evaluator abstraction
type ICloudJobEvaluator =

    /// <summary>
    ///     Asynchronously evaluates a job in the local worker.  
    /// </summary>
    /// <param name="dependencies">Local assemblies that the job depends on.</param>
    /// <param name="jobtoken">Cloud job token.</param>
    abstract Evaluate : dependencies:VagabondAssembly [] * jobtoken:ICloudJobLeaseToken -> Async<unit>

[<RequireQualifiedAccess>]
module JobEvaluator =

    /// <summary>
    ///     Asynchronously evaluates job in the local application domain.
    /// </summary>
    /// <param name="manager">Runtime resource manager.</param>
    /// <param name="currentWorker">Current worker executing job.</param>
    /// <param name="faultState">Job fault state.</param>
    /// <param name="job">Job instance to be executed.</param>
    let runJobAsync (manager : IRuntimeResourceManager) (currentWorker : IWorkerRef) 
                    (faultState : JobFaultInfo) (job : CloudJob) = async {

        let logger = manager.SystemLogger
        let jem = new JobExecutionMonitor()
        let distributionProvider = DistributionProvider.Create(currentWorker, manager, job)
        let resources = resource {
            yield! manager.ResourceRegistry
            yield jem
            yield manager
            yield distributionProvider :> IDistributionProvider
        }

        let ctx = { Resources = resources ; CancellationToken = job.CancellationToken }

        match faultState with
        | IsTargetedJobOfDeadWorker (_,w) ->
            // always throw a fault exception if dead worker
            logger.Logf LogLevel.Info "Job '%s' originally assigned to dead worker '%s'." job.Id w.Id
            let worker = Option.get job.TargetWorker // this is assumed to be 'Some' here
            let e = new FaultException(sprintf "Could not communicate with target worker '%O'." worker)
            job.Econt ctx (ExceptionDispatchInfo.Capture e)

        | FaultDeclaredByWorker(faultCount, latestError, w) ->
            logger.Logf LogLevel.Info "Job '%s' faulted %d times while executed in worker '%O'." job.Id faultCount w
            // consult user-supplied fault policy to decide on further action
            let e = latestError.Reify(prepareForRaise = false)
            match (try job.FaultPolicy.Policy faultCount e with _ -> None) with
            | None ->
                let msg = sprintf "Job '%s' given up after it faulted %d times." job.Id faultCount
                let faultException = new FaultException(msg, e)
                job.Econt ctx (ExceptionDispatchInfo.Capture faultException)

            | Some retryTimeout ->
                do! Async.Sleep (int retryTimeout.TotalMilliseconds)
                do job.StartJob ctx

        | WorkerDeathWhileProcessingJob (faultCount, latestWorker) ->
            logger.Logf LogLevel.Info "Job '%s' faulted %d times while being processed by nonresponsive worker '%O'." job.Id faultCount latestWorker
            // consult user-supplied fault policy to decide on further action
            let msg = sprintf "Job '%s' was being processed by worker '%O' which has died." job.Id latestWorker
            let e = new FaultException(msg) :> exn
            match (try job.FaultPolicy.Policy faultCount e with _ -> None) with
            | None -> job.Econt ctx (ExceptionDispatchInfo.Capture e)
            | Some retryTimeout ->
                do! Async.Sleep (int retryTimeout.TotalMilliseconds)
                do job.StartJob ctx

        | NoFault ->  
            // no faults, proceed normally  
            do job.StartJob ctx

        return! JobExecutionMonitor.AwaitCompletion jem
    }
    
    /// <summary>
    ///     loads job to local application domain and evaluates it locally.
    /// </summary>
    /// <param name="manager">Runtime resource manager.</param>
    /// <param name="currentWorker">Current worker executing job.</param>
    /// <param name="assemblies">Vagabond assemblies to be used for computation.</param>
    /// <param name="joblt">Job lease token.</param>
    let loadAndRunJobAsync (manager : IRuntimeResourceManager) (currentWorker : IWorkerRef) 
                            (assemblies : VagabondAssembly []) (joblt : ICloudJobLeaseToken) = async {

        let logger = manager.SystemLogger
        logger.Logf LogLevel.Debug "Loading assembly dependencies for job '%s'." joblt.Id
        for li in manager.AssemblyManager.LoadAssemblies assemblies do
            match li with
            | NotLoaded id -> logger.Logf LogLevel.Error "could not load assembly '%s'" id.FullName 
            | LoadFault(id, e) -> logger.Logf LogLevel.Error "error loading assembly '%s':\n%O" id.FullName e
            | Loaded _ -> ()

        logger.Logf LogLevel.Debug "Deserializing job '%s'." joblt.Id
        let! jobResult = joblt.GetJob() |> Async.Catch
        match jobResult with
        | Choice2Of2 e ->
            logger.Logf LogLevel.Error "Failed to deserialize job '%s':\n%O" joblt.Id e
            do! joblt.DeclareFaulted(ExceptionDispatchInfo.Capture e)
            do! manager.TaskManager.DeclareStatus(joblt.TaskInfo.Id, Faulted)

        | Choice1Of2 job ->
            if job.JobType = JobType.TaskRoot then
                match job.TaskInfo.Name with
                | None -> logger.Logf LogLevel.Info "Starting cloud task '%s' of type '%s'." job.TaskInfo.Id job.TaskInfo.Type
                | Some name -> logger.Logf LogLevel.Info "Starting cloud task '%s' of type '%s'." name job.TaskInfo.Type
                do! manager.TaskManager.DeclareStatus(joblt.TaskInfo.Id, Running)

            do! manager.TaskManager.IncrementJobCount(joblt.TaskInfo.Id)
            let sw = Stopwatch.StartNew()
            let! result = runJobAsync manager currentWorker joblt.FaultInfo job |> Async.Catch
            sw.Stop()
            do! manager.TaskManager.DecrementJobCount(joblt.TaskInfo.Id)

            match result with
            | Choice1Of2 () -> 
                do! joblt.DeclareCompleted ()
                logger.Logf LogLevel.Info "Completed job '%s' after %O" job.Id sw.Elapsed
            | Choice2Of2 e ->
                logger.Logf LogLevel.Error "Faulted job '%s' after %O\n%O" job.Id sw.Elapsed e
                do! joblt.DeclareFaulted (ExceptionDispatchInfo.Capture e)
    }
       

[<AutoSerializable(false)>]
type LocalJobEvaluator(manager : IRuntimeResourceManager, currentWorker : IWorkerRef) =
    interface ICloudJobEvaluator with
        member __.Evaluate (assemblies : VagabondAssembly[], jobtoken:ICloudJobLeaseToken) = async {
            return! JobEvaluator.loadAndRunJobAsync manager currentWorker assemblies jobtoken
        }

[<AutoSerializable(false)>]
type AppDomainJobEvaluator(managerF : DomainLocal<IRuntimeResourceManager * IWorkerRef>, pool : AppDomainEvaluatorPool) =

    static member Create(managerF : DomainLocal<IRuntimeResourceManager * IWorkerRef>,
                                ?initializer : unit -> unit, ?threshold : TimeSpan, 
                                ?minConcurrentDomains : int, ?maxConcurrentDomains : int) =

        let domainInitializer () = initializer |> Option.iter (fun f -> f ())
        let pool = AppDomainEvaluatorPool.Create(domainInitializer, ?threshold = threshold, 
                                                    ?minimumConcurrentDomains = minConcurrentDomains,
                                                    ?maximumConcurrentDomains = maxConcurrentDomains)

        new AppDomainJobEvaluator(managerF, pool)

    interface ICloudJobEvaluator with
        member __.Evaluate (assemblies : VagabondAssembly[], jobtoken:ICloudJobLeaseToken) = async {
            // avoid capturing evaluator in closure
            let managerF = managerF
            let eval () = async { 
                let manager, currentWorker = managerF.Value 
                return! JobEvaluator.loadAndRunJobAsync manager currentWorker assemblies jobtoken 
            }

            return! pool.EvaluateAsync(jobtoken.TaskInfo.Dependencies, eval ())
        }

    interface IDisposable with
        member __.Dispose () = (pool :> IDisposable).Dispose()