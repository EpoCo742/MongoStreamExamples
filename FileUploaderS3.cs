using System;
using System.IO;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Logging;

public class FileToS3Uploader
{
    private readonly AmazonS3Client _s3Client;
    private readonly ILogger<FileToS3Uploader> _logger; // Using a logger for better production support

    public FileToS3Uploader(AmazonS3Client s3Client, ILogger<FileToS3Uploader> logger)
    {
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task UploadFileToS3Async(string filePath, string bucketName, string objectKey)
    {
        if (string.IsNullOrWhiteSpace(filePath)) throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        if (string.IsNullOrWhiteSpace(bucketName)) throw new ArgumentException("Bucket name cannot be null or empty.", nameof(bucketName));
        if (string.IsNullOrWhiteSpace(objectKey)) throw new ArgumentException("Object key cannot be null or empty.", nameof(objectKey));

        const int chunkSize = 1048576; // 1MB
        var initiateRequest = new InitiateMultipartUploadRequest
        {
            BucketName = bucketName,
            Key = objectKey
        };

        var initiateResponse = await _s3Client.InitiateMultipartUploadAsync(initiateRequest);
        List<UploadPartResponse> uploadResponses = new List<UploadPartResponse>();
        int partNumber = 1;

        try
        {
            using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            {
                byte[] buffer = new byte[chunkSize];
                int bytesRead;

                while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    // Ensure the last buffer is right sized
                    byte[] actualBuffer = bytesRead == chunkSize ? buffer : buffer.Take(bytesRead).ToArray();

                    using var memoryStream = new MemoryStream(actualBuffer);
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
