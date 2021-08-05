using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace ThroughputTestFunction
{
    public static class ReceiverFunction
    {
        [FunctionName("ReceiverFunction")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string eventNumber = req.Query["actionId"];
            string eventTime;


            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            eventTime = data?.eventTime;

            string response = $"eventNumber: {eventNumber}; eventTime: {eventTime}";
            log.LogInformation(response);

            return new OkObjectResult($"Success: {response}");
        }
    }
}
