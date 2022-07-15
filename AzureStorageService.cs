using _3ai.solutions.Core.Interfaces;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using System.IO.Compression;

namespace _3ai.solutions.AzureStorage
{
    public class AzureStorageService : IStorageService
    {
        private readonly string _connectionString; 
        private readonly string _accountKey;
        private readonly string _accountName;
        private readonly int _SASTTL;

        public AzureStorageService(AzureStorageOptions azureStorageSettings)
        {
            _connectionString = azureStorageSettings.ConnectionString;
            _accountKey = azureStorageSettings.AccountKey;
            _accountName = azureStorageSettings.AccountName; 
            _SASTTL = azureStorageSettings.SASTTL;
        }

        public async Task<string> AddAsync(string relativePath, byte[] data, string contentType, CancellationToken token = default)
        {
            string uri = "";
            if (!string.IsNullOrEmpty(relativePath))
            {
                using MemoryStream ms = new();
                KeyValuePair<string, string> file = GetFileParts(relativePath);
                BlockBlobClient blockBlobClient = AccessFile(file);
                Azure.Storage.Blobs.Models.BlobHttpHeaders? bhh = null;
                if (!string.IsNullOrEmpty(contentType))
                {
                    bhh = new Azure.Storage.Blobs.Models.BlobHttpHeaders { ContentType = contentType };
                }
                using (MemoryStream stream = new(data))
                {
                    await blockBlobClient.UploadAsync(stream, bhh, cancellationToken: token);
                }
                uri = blockBlobClient.Uri.AbsoluteUri;
            }
            return uri;
        }

        public async Task<bool> DeleteAsync(string remotePath, CancellationToken token = default)
        {
            bool success = false;
            if (!string.IsNullOrEmpty(remotePath))
            {
                KeyValuePair<string, string> file = GetFileParts(remotePath);
                BlockBlobClient blockBlobClient = AccessFile(file);
                success = await blockBlobClient.DeleteIfExistsAsync(cancellationToken: token);
            }
            return success;
        }

        public async Task<byte[]> GetAsync(string remotePath, CancellationToken token = default)
        {
            if (!string.IsNullOrEmpty(remotePath))
            {
                KeyValuePair<string, string> file = GetFileParts(remotePath);
                BlockBlobClient blockBlobClient = AccessFile(file);
                if (await blockBlobClient.ExistsAsync(cancellationToken: token))
                {
                    using MemoryStream ms = new();
                    await blockBlobClient.DownloadToAsync(ms, cancellationToken: token);
                    var props = await blockBlobClient.GetPropertiesAsync(cancellationToken: token);
                    return ms.ToArray();
                }
            }
            return Array.Empty<byte>();
        }

        public string GetURI(string remotePath)
        {
            if (!string.IsNullOrEmpty(remotePath))
            {
                string[] remotePaths = remotePath.Split(",", StringSplitOptions.RemoveEmptyEntries);
                KeyValuePair<string, string> file = GetFileParts(remotePaths[0]);
                BlockBlobClient blockBlobClient = AccessFile(file);
                return blockBlobClient.Uri.AbsoluteUri;
            }
            return remotePath;
        }

        public string GetAccessURL(string remotePath, string ip)
        {
            string[] parts = remotePath.Split("/");
            string containerName = parts.First();
            string blobName = string.Join("/", parts.Skip(1));
            BlobSasBuilder blobSasBuilder = new()
            {
                ExpiresOn = DateTimeOffset.UtcNow.AddHours(_SASTTL),
                BlobContainerName = containerName,
                BlobName = blobName,
                StartsOn = DateTimeOffset.UtcNow,
                Resource = "b",
                Protocol = SasProtocol.Https,
                IPRange = SasIPRange.Parse(ip),
            };
            blobSasBuilder.SetPermissions(BlobSasPermissions.All);

            StorageSharedKeyCredential storageSharedKeyCredential = new(_accountName, _accountKey);
            BlobSasQueryParameters sasQueryParameters = blobSasBuilder.ToSasQueryParameters(storageSharedKeyCredential);

            UriBuilder fullUri = new()
            {
                Scheme = blobSasBuilder.Protocol.ToString(),
                Host = $"{_accountName}.blob.core.windows.net",
                Path = string.Join("/", blobSasBuilder.BlobContainerName, blobSasBuilder.BlobName),
                Query = sasQueryParameters.ToString()
            };

            return fullUri.ToString();
        }

        public async Task<bool> ZipAsync(string remotePaths, CancellationToken token = default)
        {
            bool success = false;
            if (!string.IsNullOrEmpty(remotePaths))
            {
                string[] remotePathParts = remotePaths.Split(",", StringSplitOptions.RemoveEmptyEntries);
                KeyValuePair<string, string> file = GetFileParts(remotePathParts[0]);
                BlockBlobClient blockBlobClient = AccessFile(file);
                using MemoryStream ms = new();
                using (ZipArchive zipFile = new(ms, ZipArchiveMode.Create))
                {
                    for (int i = 1; i < remotePathParts.Length; i++)
                    {
                        file = GetFileParts(remotePathParts[i]);
                        BlockBlobClient blockBlobClientI = AccessFile(file);
                        int splitIndex = file.Value.LastIndexOf("/");
                        ZipArchiveEntry entry = zipFile.CreateEntry(file.Value[(splitIndex + 1)..], CompressionLevel.Optimal);
                        using var innerFile = entry.Open();
                        await blockBlobClientI.DownloadToAsync(innerFile, token);
                    }
                }
                ms.Seek(0, SeekOrigin.Begin);
                await blockBlobClient.UploadAsync(ms, cancellationToken: token);
                success = true;
            }
            return success;
        }

        private BlockBlobClient AccessFile(KeyValuePair<string, string> file)
        {
            BlobServiceClient blobServiceClient = new(_connectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(file.Key);
            containerClient.CreateIfNotExists();
            BlockBlobClient blockBlobClient = containerClient.GetBlockBlobClient(file.Value);
            return blockBlobClient;
        }

        private static KeyValuePair<string, string> GetFileParts(string keyName)
        {
            string[] parts = keyName.Split("/");
            return new KeyValuePair<string, string>(parts.First(), string.Join("/", parts.Skip(1)));
        }
    }
}