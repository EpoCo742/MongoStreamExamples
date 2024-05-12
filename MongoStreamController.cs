using Microsoft.AspNetCore.Mvc;
using System.Threading;
using System.Threading.Tasks;

[ApiController]
[Route("[controller]")]
public class DataController : ControllerBase
{
    private readonly DataSenderService _dataSenderService;

    public DataController(DataSenderService dataSenderService)
    {
        _dataSenderService = dataSenderService;
    }

    [HttpPost("send-data")]
    public async Task<IActionResult> SendData(CancellationToken cancellationToken)
    {
        try
        {
            await _dataSenderService.SendDataAsync(cancellationToken);
            return Ok("Data sent successfully");
        }
        catch (OperationCanceledException)
        {
            return StatusCode(499, "Client closed request"); // HTTP 499 Client Closed Request (nonstandard status code introduced by nginx)
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"An error occurred: {ex.Message}");
        }
    }
}
