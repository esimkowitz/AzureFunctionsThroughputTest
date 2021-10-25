using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace ReceiverWebApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ReceiverController : ControllerBase
    {
        private readonly ILogger<ReceiverController> _logger;

        private static readonly JsonSerializerOptions jsonSerializerOptions = new JsonSerializerOptions()
        {
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        public ReceiverController(ILogger<ReceiverController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public async Task<ActionResult<string>> Get()
        {
            _logger.LogInformation("C# ASP.NET API processed a GET request.");
            return await ProcessRequest();
        }

        [HttpPost]
        public async Task<ActionResult<string>> Post()
        {
            _logger.LogInformation("C# ASP.NET API processed a POST request.");
            return await ProcessRequest();
        }

        private async Task<string> ProcessRequest()
        {
            HttpRequest req = HttpContext.Request;
            _logger.LogInformation($"queryString: {req.QueryString}");

            string eventNumber = req.Query["actionId"];
            var data = await JsonSerializer.DeserializeAsync<RequestBody>(req.Body, jsonSerializerOptions, CancellationToken.None);
            string eventTime = data?.EventTime;

            string response = $"eventNumber: {eventNumber}; eventTime: {eventTime}";
            _logger.LogInformation(response);
            return response;
        }
    }

    public class RequestBody
    {
        public string EventTime { get; set; }
    }
}
