using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace SenderFunction
{
    public static class SenderFunction
    {
        const int numEventsDefault = 50;
        const int numRunsDefault = 3;
        const int intervalSecondsDefault = 10;

        public struct OrchestratorInput
        {
            public Uri TargetUri;
            public int NumEvents;
            public int NumRuns;
            public TimeSpan Interval;

            public OrchestratorInput(Uri targetUri, int numEvents, int numRuns, TimeSpan interval)
            {
                TargetUri = targetUri;
                NumEvents = numEvents;
                NumRuns = numRuns;
                Interval = interval;
            }
            public OrchestratorInput(string targetUri, int numEvents, int numRuns, int intervalSeconds) : this(new Uri(targetUri), numEvents, numRuns, TimeSpan.FromSeconds(intervalSeconds)) { }

            public override string ToString() => $"OrchestratorInput: {{ TargetUri: \"{TargetUri}\", NumEvents: {NumEvents}, NumRuns: {NumRuns}, Interval: {Interval.TotalSeconds} }}";
        }

        public struct ActivityInput
        {
            public Uri TargetUri;
            public string ActivityId;
            public DateTime QueueTime;

            public ActivityInput(Uri targetUri, string activityId, DateTime queueTime)
            {
                TargetUri = targetUri;
                ActivityId = activityId;
                QueueTime = queueTime;
            }
            public ActivityInput(Uri targetUri, int runId, int eventId, DateTime queueTime) : this(targetUri, $"{runId}_{eventId}", queueTime) { }

            public override string ToString() => $"ActivityInput: {{ TargetUri: \"{TargetUri.AbsoluteUri}\", ActivityId: \"{ActivityId}\", QueueTime: {QueueTime} }}";
        }

        [FunctionName("SenderFunction")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger log)
        {
            OrchestratorInput input = context.GetInput<OrchestratorInput>(); 
            log.LogInformation($"SenderFunction: Starting run {context.InstanceId}, input {input}");

            List<string> outputs = new List<string>();

            for (int runId = 0; runId < input.NumRuns; runId++)
            {
                Task<string>[] tasks = new Task<string>[input.NumEvents];

                for (int eventId = 0; eventId < input.NumEvents; eventId++)
                {
                    tasks[eventId] = context.CallActivityAsync<string>("SenderFunction_Worker", new ActivityInput(input.TargetUri, runId, eventId, context.CurrentUtcDateTime));
                }

                await Task.WhenAll(tasks);
                outputs.AddRange(tasks.Select((t) => t.Result));
                log.LogInformation($"SenderFunction: run {runId} workers have completed");

                DateTime deadline = context.CurrentUtcDateTime.Add(input.Interval);
                await context.CreateTimer(deadline, CancellationToken.None);
            }

            return outputs;
        }

        [FunctionName("SenderFunction_Worker")]
        public static async Task<string> WorkerAsync([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            DateTime currentTime = DateTime.UtcNow;
            var input = context.GetInput<ActivityInput>();
            string responseStr;
            using (WebClient webClient = new WebClient())
            {
                webClient.Headers[HttpRequestHeader.ContentType] = "application/json";
                webClient.QueryString.Add("actionId", input.ActivityId);
                try
                {
                    responseStr = await webClient.UploadStringTaskAsync(input.TargetUri, JsonSerializer.Serialize(new Dictionary<string, DateTime>() { { "eventTime", input.QueueTime } }));
                }
                catch (Exception ex)
                {
                    responseStr = $"POST call failed with exception {ex.GetType()} and message \"{ex.Message}\"";
                }
            }

            string tempResponse = $"SenderFunction_Worker: input {input}, currentTime {currentTime}, response: \"{responseStr}\"";
            log.LogInformation(tempResponse);
            return tempResponse;
        }

        [FunctionName("SenderFunction_HttpStart")]
        public static async Task<IActionResult> HttpStart(
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

            if (!int.TryParse(req.Query["numRuns"], out int numRuns))
            {
                numRuns = numRunsDefault;
            }

            if (!int.TryParse(req.Query["intervalSeconds"], out int intervalSeconds))
            {
                intervalSeconds = intervalSecondsDefault;
            }

            log.LogInformation($"SenderFunction_HttpStart: New request sent with values targetUri: \"{targetUri}\", numEvents: {numEvents}, numRuns: {numRuns}, intervalSeconds: {intervalSeconds}");
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("SenderFunction", null, new OrchestratorInput(targetUri, numEvents, numRuns, intervalSeconds));

            log.LogInformation($"SenderFunction_HttpStart: Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}