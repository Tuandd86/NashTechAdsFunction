using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Formats.Jpeg;
using SixLabors.ImageSharp.Processing;

namespace NashTechAdsImageProcessing
{
    public static class ImageProcessor
    {
        [FunctionName("ImageProcessor")]
        public async static void Run([QueueTrigger("%QueueName%", Connection = "AzureStorageConnection")]string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {myQueueItem}");

            var messageParts = myQueueItem.Split("###", StringSplitOptions.None);
            var adId = int.Parse(messageParts[0]);
            var imageName = messageParts[1];

            var storeageConnectionString = Environment.GetEnvironmentVariable("AzureStorageConnection");
            var containerName = Environment.GetEnvironmentVariable("AzureStorageBlobContainer");

            var cloudStorageAccount = CloudStorageAccount.Parse(storeageConnectionString);
            var blobClient = cloudStorageAccount.CreateCloudBlobClient();
            var cloudBlobContainer = blobClient.GetContainerReference(containerName);
            var blockBlob = cloudBlobContainer.GetBlockBlobReference(imageName);

            var thumbnailName = $"{Guid.NewGuid()}.jpg";
            var thumbnailBlob = cloudBlobContainer.GetBlockBlobReference(thumbnailName);

            using (var inputStream = new MemoryStream())
            using (var outputStream = new MemoryStream())
            {
                await blockBlob.DownloadToStreamAsync(inputStream);
                inputStream.Seek(0, SeekOrigin.Begin);
                using (var image = Image.Load(inputStream))
                {
                    image.Mutate(x => x.Resize(100, 100));
                    image.Save(outputStream, new JpegEncoder());
                }

                outputStream.Seek(0, SeekOrigin.Begin);
                await thumbnailBlob.UploadFromStreamAsync(outputStream);
            }

            UpdateAd(adId, thumbnailName);
        }

        private static void UpdateAd(int adId, string thumbnail)
        {
            var connectionString = Environment.GetEnvironmentVariable("DbConnection");
            using (var connection = new SqlConnection(connectionString))
            {
                string sql = $"UPDATE Ads SET Thumbnail = '{thumbnail}', Status = 1 WHERE Id = {adId}";
                using (var command = new SqlCommand(sql, connection))
                {
                    command.CommandType = CommandType.Text;
                    connection.Open();
                    command.ExecuteNonQuery();
                    connection.Close();
                }
            }
        }
    }
}
