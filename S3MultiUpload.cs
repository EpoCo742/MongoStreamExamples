using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using MongoDB.Bson;
using MongoDB.Driver;
using Microsoft.Extensions.Logging;

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
            BatchSize = 1000, // Adjust the batch size based on your scenario
            NoCursorTimeout = true
        };

        List<UploadPartResponse> uploadResponses = new List<UploadPartResponse>();
        int partNumber = 1;

        try
        {
            await using (var cursor = await _collection.FindAsync(filter, options))
            {
                while (await cursor.MoveNextAsync())
                {
                    foreach (var doc in cursor.Current)
                    {
                        var json = doc.ToJson(new MongoDB.Bson.IO.JsonWriterSettings { OutputMode = MongoDB.Bson.IO.JsonOutputMode.Strict }) + Environment.NewLine;
                        using var memoryStream = new MemoryStream();
                        using (var writer = new StreamWriter(memoryStream))
                        {
                            await writer.WriteAsync(json);
                            await writer.FlushAsync();
                            memoryStream.Position = 0; // Reset the position to the beginning of the stream

                            var uploadRequest = new UploadPartRequest
                            {
                                BucketName = bucketName,
                                Key = objectKey,
                                UploadId = initiateResponse.UploadId,
                                PartNumber = partNumber++,
                                InputStream = memoryStream
                            };

                            var uploadResponse = await _s3Client.UploadPartAsync(uploadRequest);
                            uploadResponses.Add(uploadResponse);
                        }
                    }
                }
            }

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
            await _s3Client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                UploadId = initiateResponse.UploadId
            });

            throw;
        }
    }
}
