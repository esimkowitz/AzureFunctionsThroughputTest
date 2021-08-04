using System;
using System.Collections.Generic;
using System.Linq;
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
            public int NumEvents;
            public int NumRuns;
            public TimeSpan Interval;

            public OrchestratorInput(int numEvents, int numRuns, TimeSpan interval)
            {
                NumEvents = numEvents;
                NumRuns = numRuns;
                Interval = interval;
            }
            public OrchestratorInput(int numEvents, int numRuns, int intervalSeconds)
            {
                NumEvents = numEvents;
                NumRuns = numRuns;
                Interval = TimeSpan.FromSeconds(intervalSeconds);
            }

            public override string ToString() => $"OrchestratorInput: {{ NumEvents: {NumEvents}, NumRuns: {NumRuns}, Interval: {Interval.TotalSeconds} }}";
        }

        public struct ActivityInput
        {
            public string ActivityId;
            public DateTime QueueTime;

            public ActivityInput(string activityId, DateTime queueTime)
            {
                ActivityId = activityId;
                QueueTime = queueTime;
            }
            public ActivityInput(int runId, int eventId, DateTime queueTime)
            {
                ActivityId = $"{runId}_{eventId}";
                QueueTime = queueTime;
            }

            public override string ToString() => $"ActivityInput: {{ ActivityId: \"{ActivityId}\", QueueTime: {QueueTime} }}";
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
                    tasks[eventId] = context.CallActivityAsync<string>("SenderFunction_Worker", new ActivityInput(runId, eventId, context.CurrentUtcDateTime));
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
        public static string Worker([ActivityTrigger] ActivityInput input, ILogger log)
        {
            DateTime currentTime = DateTime.UtcNow;
            string tempResponse = $"SenderFunction_Worker: input {input}, currentTime {currentTime}";
            log.LogInformation(tempResponse);
            return tempResponse;
        }

        [FunctionName("SenderFunction_HttpStart")]
        public static async Task<IActionResult> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
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

            log.LogInformation($"SenderFunction_HttpStart: New request sent with values numEvents: {numEvents}, numRuns: {numRuns}, intervalSeconds: {intervalSeconds}");
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("SenderFunction", null, new OrchestratorInput(numEvents, numRuns, intervalSeconds));

            log.LogInformation($"SenderFunction_HttpStart: Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}