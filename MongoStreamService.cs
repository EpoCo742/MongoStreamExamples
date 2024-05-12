using MongoDB.Driver;
using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

public class DataSenderService
{
    private readonly IMongoCollection<BsonDocument> _collection;
    private readonly HttpClient _httpClient;
    private readonly string _targetApiUrl;
    private readonly ILogger _logger;

    public DataSenderService(IHttpClientFactory httpClientFactory, string connectionString, string databaseName, string collectionName, string targetApiUrl, ILogger logger)
    {
        var client = new MongoClient(connectionString);
        var database = client.GetDatabase(databaseName);
        _collection = database.GetCollection<BsonDocument>(collectionName);
        _httpClient = httpClientFactory.CreateClient();
        _targetApiUrl = targetApiUrl;
        _logger = logger;
    }

    public async Task SendDataAsync(CancellationToken cancellationToken)
    {
        var filter = Builders<BsonDocument>.Filter.Empty;
        var options = new FindOptions<BsonDocument> { BatchSize = 1000 };

        try
        {
            using (var memoryStream = new MemoryStream())
            {
                using (var writer = new StreamWriter(memoryStream, Encoding.UTF8, 1024, true))
                {
                    using (var cursor = await _collection.FindAsync(filter, options, cancellationToken))
                    {
                        while (await cursor.MoveNextAsync(cancellationToken))
                        {
                            foreach (var doc in cursor.Current)
                            {
                                var line = doc.ToJson();
                                await writer.WriteLineAsync(line);
                                cancellationToken.ThrowIfCancellationRequested();
                            }
                        }
                    }
                    await writer.FlushAsync();
                }

                memoryStream.Position = 0;  // Reset the stream position to the beginning for reading
                await SendDataStream(memoryStream, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred while fetching or processing data");
            throw;
        }
    }

    private async Task SendDataStream(MemoryStream dataStream, CancellationToken cancellationToken)
    {
        try
        {
            using (var content = new StreamContent(dataStream))
            {
                content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");
                var response = await _httpClient.PostAsync(_targetApiUrl, content, cancellationToken);
                response.EnsureSuccessStatusCode();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send data");
            throw;
        }
    }
}
