using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace SenderFunction
{
    public class SenderFunction
    {
        const int numEventsDefault = 50;
        const int numEventsPerWorkerDefault = 5;
        const int numRunsDefault = 3;
        const int intervalSecondsDefault = 10;

        public struct OrchestratorInput
        {
            public Uri TargetUri;
            public int NumEvents;
            public int NumEventsPerWorker;
            public int NumRuns;
            public TimeSpan Interval;

            public OrchestratorInput(string targetUri, int numEvents, int numEventsPerWorker, int numRuns, int intervalSeconds)
            {
                TargetUri = new Uri(targetUri);
                NumEvents = numEvents;
                NumEventsPerWorker = numEventsPerWorker;
                NumRuns = numRuns;
                Interval = TimeSpan.FromSeconds(intervalSeconds);
            }

            public override string ToString() => $"OrchestratorInput: {{ TargetUri: \"{TargetUri}\", NumEvents: {NumEvents}, NumEventsPerWorker: {NumEventsPerWorker}, NumRuns: {NumRuns}, Interval: {Interval.TotalSeconds} }}";
        }

        public struct ActivityInput
        {
            public Uri TargetUri;
            public string FunctionCode;
            public string WorkerId;
            public int NumEvents;
            public int NumRuns;
            public TimeSpan Interval;

            public DateTime QueueTime;

            public ActivityInput(Uri targetUri, string functionCode, string instanceId, int workerId, int numEvents, int numRuns, TimeSpan interval, DateTime queueTime) {
                TargetUri = targetUri;
                FunctionCode = functionCode;
                WorkerId = $"{instanceId.Substring(0, 5)}_{workerId}";
                NumRuns = numRuns;
                NumEvents = numEvents;
                Interval = interval;
                QueueTime = queueTime;
            }
        }

        private readonly TelemetryClient telemetryClient;
        private readonly HttpClient httpClient;

        public SenderFunction(HttpClient httpClient, TelemetryConfiguration telemetryConfiguration)
        {
            this.telemetryClient = new TelemetryClient(telemetryConfiguration);
            this.httpClient = httpClient;
        }

        [FunctionName("SenderFunction")]
        public async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger log)
        {
            OrchestratorInput input = context.GetInput<OrchestratorInput>(); 
            log.LogInformation($"SenderFunction: Starting run {context.InstanceId}, input {input}");

            var queryStringParams = input.TargetUri.ParseQueryString();
            Uri targetUriMinusQuery = new Uri(input.TargetUri.GetLeftPart(UriPartial.Path));
            string functionCode = queryStringParams.Get("code");

            List<string> outputs = new List<string>();
            
            int numWorkers = (int)Math.Ceiling(input.NumEvents / (double)input.NumEventsPerWorker);
            Task<string>[] tasks = new Task<string>[numWorkers];

            for (int workerId = 0; workerId < numWorkers; workerId++)
            {
                tasks[workerId] = context.CallActivityAsync<string>(
                    "SenderFunction_Worker",
                    new ActivityInput(
                        targetUriMinusQuery,
                        functionCode,
                        context.InstanceId,
                        workerId,
                        input.NumEventsPerWorker,
                        input.NumRuns,
                        input.Interval,
                        context.CurrentUtcDateTime));
            }

            await Task.WhenAll(tasks);
            outputs.AddRange(tasks.Select((t) => t.Result));

            return outputs;
        }

        [FunctionName("SenderFunction_Worker")]
        public async Task<string> WorkerAsync([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            var input = context.GetInput<ActivityInput>();
            log.LogInformation($"{input.WorkerId} starting with target \"{input.TargetUri}\" and functionCode \"{input.FunctionCode}\"");
            log.LogInformation($"numRuns: {input.NumRuns}, numEvents: {input.NumEvents}");

            ConcurrentDictionary<string, bool> results = new ConcurrentDictionary<string, bool>();
            string response = "";

            var requestContent = JsonContent.Create(new Dictionary<string, DateTime>() { { "eventTime", input.QueueTime } });
            UriBuilder uriBuilder = new UriBuilder(input.TargetUri);
            QueryString queryString = new QueryString();
            if (!string.IsNullOrEmpty(input.FunctionCode))
            {
                queryString = queryString.Add("code", input.FunctionCode);
            }


            for (int runId = 0; runId < input.NumRuns; runId++)
            {
                log.LogInformation($"{input.WorkerId} starting run {runId}");
                string workerRunId = $"{input.WorkerId}_{runId}";

                var parallelInputs = Enumerable.Range(0, input.NumEvents);

                SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());

                await parallelInputs.AsyncParallelForEach(async (int eventId) =>
                {
                    //log.LogInformation($"{workerRunId} starting event {eventId}");
                    Stopwatch stopwatch = new Stopwatch();
                    stopwatch.Start();
                    bool isSuccess = true;
                    string activityId = $"{workerRunId}_{eventId}";

                    var newQueryString = queryString.Add("actionId", activityId);

                    try
                    {
                        var response = await httpClient.PostAsync(input.TargetUri + newQueryString.ToUriComponent(), requestContent);
                        response.EnsureSuccessStatusCode();
                        this.telemetryClient.GetMetric("SenderFunctionPostSuccess").TrackValue(1);
                        //log.LogInformation($"{activityId} POST call succeeded with response \"{responseStr}\"");
                    }
                    catch (Exception ex)
                    {
                        log.LogError($"{activityId} POST call failed with exception {ex.GetType()} and message \"{ex.Message}\"");
                        isSuccess = false; 
                        this.telemetryClient.GetMetric("SenderFunctionPostFail").TrackValue(1);

                    }
                    //log.LogInformation($"{activityId} {(isSuccess ? "succeeded" : "failed")} in {stopwatch.ElapsedMilliseconds} ms");
                    this.telemetryClient.GetMetric("ResponseTime").TrackValue(stopwatch.ElapsedMilliseconds);
                    results.TryAdd(activityId, isSuccess);
                }, 20, TaskScheduler.FromCurrentSynchronizationContext());
                log.LogInformation($"{workerRunId}: numresults: {results.Count}");
                foreach (KeyValuePair<string, bool> result in results)
                {
                    response += $"{result.Key}: {result.Value}; ";
                }
                results.Clear();
                await Task.Delay(input.Interval);
            }
            
            return response;
        }

        [FunctionName("SenderFunction_HttpStart")]
        public async Task<IActionResult> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            string targetUri = req.Query["targetUri"];
            if (!Uri.IsWellFormedUriString(targetUri, UriKind.Absolute))
            {
                return new BadRequestObjectResult("The query parameter \"targetUri\" is not formatted correctly or is missing");
            }

            if (!int.TryParse(req.Query["numEvents"], out int numEvents))
            {
                numEvents = numEventsDefault;
            }


            if (!int.TryParse(req.Query["numEventsPerWorker"], out int numEventsPerWorker))
            {
                numEventsPerWorker = numEventsPerWorkerDefault;
            }

            if (!int.TryParse(req.Query["numRuns"], out int numRuns))
            {
                numRuns = numRunsDefault;
            }

            if (!int.TryParse(req.Query["intervalSeconds"], out int intervalSeconds))
            {
                intervalSeconds = intervalSecondsDefault;
            }

            log.LogInformation($"SenderFunction_HttpStart: New request sent with values targetUri: \"{targetUri}\", numEvents: {numEvents}, numEventsPerWorker: {numEventsPerWorker}, numRuns: {numRuns}, intervalSeconds: {intervalSeconds}");
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync(
                "SenderFunction",
                null,
                new OrchestratorInput(
                    targetUri,
                    numEvents,
                    numEventsPerWorker,
                    numRuns,
                    intervalSeconds));

            log.LogInformation($"SenderFunction_HttpStart: Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

    }
    public static class Extensions
    {
        public static Task AsyncParallelForEach<T>(this IEnumerable<T> source, Func<T, Task> body, int maxDegreeOfParallelism = DataflowBlockOptions.Unbounded, TaskScheduler scheduler = null)
        {
            var options = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = maxDegreeOfParallelism
            };
            if (scheduler != null)
                options.TaskScheduler = scheduler;

            var block = new ActionBlock<T>(body, options);

            foreach (var item in source)
                block.Post(item);

            block.Complete();
            return block.Completion;
        }
    }
}