namespace _3ai.solutions.AzureStorage
{
    public record AzureStorageOptions
    {
        public string ConnectionString { get; set; } = string.Empty;
        public string AccountKey { get; set; } = string.Empty;
        public string AccountName { get; set; } = string.Empty;
        public int SASTTL { get; set; }
    }
}