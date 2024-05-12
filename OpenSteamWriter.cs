public async Task StreamDataToS3Async()
{
    var bucketName = "your-s3-bucket-name";
    var objectKey = "your-desired-object-key";
    var requestUri = $"https://{bucketName}.s3.amazonaws.com/{objectKey}";

    var filter = Builders<BsonDocument>.Filter.Empty;
    var options = new FindOptions<BsonDocument> { BatchSize = 1000 };

    var httpRequestMessage = new HttpRequestMessage(HttpMethod.Put, requestUri);
    httpRequestMessage.Headers.Add("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
    httpRequestMessage.Headers.Add("Authorization", GenerateAwsSignature()); // You need to implement this method
    httpRequestMessage.Headers.Add("x-amz-date", DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ"));

    httpRequestMessage.Content = new PushStreamContent(async (stream, httpContent, transportContext) =>
    {
        using (var writer = new StreamWriter(stream))
        {
            using (var cursor = await _collection.FindAsync(filter, options))
            {
                while (await cursor.MoveNextAsync())
                {
                    var currentBatch = cursor.Current;
                    foreach (var doc in currentBatch)
                    {
                        var json = doc.ToJson(new MongoDB.Bson.IO.JsonWriterSettings { OutputMode = MongoDB.Bson.IO.JsonOutputMode.Strict }) + Environment.NewLine;
                        await writer.WriteAsync(json);
                    }
                }
            }
        }
    }, "application/octet-stream");

    // Send the request using HttpClient
    using (var response = await _httpClient.SendAsync(httpRequestMessage, HttpCompletionOption.ResponseHeadersRead))
    {
        response.EnsureSuccessStatusCode();
        // Optionally handle the response, e.g., logging upload success
    }
}

private string GenerateAwsSignature()
{
    // Implement AWS Signature V4 generation here
    return "Generated AWS Signature";
}
