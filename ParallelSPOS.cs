using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using MongoDB.Bson;
using MongoDB.Driver;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading;

public class MongoToS3Uploader
{
    private readonly IMongoCollection<BsonDocument> _collection;
    private readonly AmazonS3Client _s3Client;
    private readonly ILogger<MongoToS3Uploader> _logger;

    public MongoToS3Uploader(IMongoCollection<BsonDocument> collection, AmazonS3Client s3Client, ILogger<MongoToS3Uploader> logger)
    {
        _collection = collection ?? throw new ArgumentNullException(nameof(collection));
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task StreamDataToS3Async(string bucketName, string objectKey)
    {
        if (string.IsNullOrWhiteSpace(bucketName))
            throw new ArgumentException("Bucket name cannot be null or empty.", nameof(bucketName));
        if (string.IsNullOrWhiteSpace(objectKey))
            throw new ArgumentException("Object key cannot be null or empty.", nameof(objectKey));

        var initiateRequest = new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = objectKey
        };
        var initiateResponse = await _s3Client.InitiateMultipartUploadAsync(initiateRequest);

        var filter = Builders<BsonDocument>.Filter.Empty;
        var options = new FindOptions<BsonDocument, BsonDocument>
        {
            BatchSize = 1000
        };

        ConcurrentBag<UploadPartResponse> uploadResponses = new ConcurrentBag<UploadPartResponse>();
        int partNumber = 1;
        var uploadingTasks = new List<Task>();
        var cancellationTokenSource = new CancellationTokenSource();

        try
        {
            using (var cursor = await _collection.FindAsync(filter, options))
            {
                while (await cursor.MoveNextAsync())
                {
                    cancellationTokenSource.Token.ThrowIfCancellationRequested();
                    var batch = cursor.Current;
                    var uploadTask = UploadBatchAsync(batch, bucketName, objectKey, initiateResponse.UploadId, Interlocked.Increment(ref partNumber), uploadResponses);
                    uploadingTasks.Add(uploadTask);

                    // Start a new task if the previous one is completed, maintaining a manageable number of concurrent uploads
                    if (uploadingTasks.Count >= 5)
                    {
                        var completedTask = await Task.WhenAny(uploadingTasks);
                        uploadingTasks.Remove(completedTask);
                    }
                }
            }

            // Wait for all ongoing upload tasks to complete
            await Task.WhenAll(uploadingTasks);

            var completeRequest = new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                UploadId = initiateResponse.UploadId,
                PartETags = uploadResponses.Select(r => new PartETag { PartNumber = r.PartNumber, ETag = r.ETag }).ToList()
            };

            await _s3Client.CompleteMultipartUploadAsync(completeRequest);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "An error occurred during the multipart upload. Aborting upload.");
            cancellationTokenSource.Cancel();

            await _s3Client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                UploadId = initiateResponse.UploadId
            });

            throw;
        }
    }

    private async Task UploadBatchAsync(IEnumerable<BsonDocument> batch, string bucketName, string objectKey, string uploadId, int partNumber, ConcurrentBag<UploadPartResponse> uploadResponses)
    {
        using var memoryStream = new MemoryStream();
        using (var writer = new StreamWriter(memoryStream, Encoding.UTF8, 1024, true))
        {
            foreach (var doc in batch)
            {
                var json = doc.ToJson(new MongoDB.Bson.IO.JsonWriterSettings { OutputMode = MongoDB.Bson.IO.JsonOutputMode.Strict }) + Environment.NewLine;
                await writer.WriteAsync(json);
            }
            await writer.FlushAsync();
            memoryStream.Position = 0;

            var uploadRequest = new UploadPartRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                UploadId = uploadId,
                PartNumber = partNumber,
                InputStream = memoryStream
            };

            var uploadResponse = await _s3Client.UploadPartAsync(uploadRequest);
            uploadResponses.Add(uploadResponse);
        }
    }
}
