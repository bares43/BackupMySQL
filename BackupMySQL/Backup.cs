using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using MySql.Data.MySqlClient;

namespace BackupMySQL {
    public static class Backup {

        private static IConfigurationRoot _config;
        private static ILogger _logger;

        [FunctionName("Backup")]
        public static async Task Run([TimerTrigger("0 0 15 * * 0")]TimerInfo myTimer, ILogger log, ExecutionContext context) {

            _logger = log;
            _config = BuildConfig(context);

            var toBackup = GetDatabases();

            foreach (var database in toBackup) {
                await BackupDatabase(database.Key, database.Value);
            }
        }

        private static IConfigurationRoot BuildConfig(ExecutionContext context) {
            return new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();
        }

        private static async Task BackupDatabase(string planName, string connectionString) {

            try {
                var sqlDump = GetDatabaseSqlDump(planName, connectionString);
                await UploadSqlDumpToStorage(planName, sqlDump);

                _logger.LogInformation($"Backup of {planName} successful");
            } catch (Exception e) {
                _logger.LogError(e, $"Database: {planName}, Message: {e.Message}");
            }
        }

        private static string GetDatabaseSqlDump(string planName, string connectionString) {

            var sql = string.Empty;

            using (var connection = new MySqlConnection(connectionString)) {
                using (var command = new MySqlCommand()) {
                    using (var backup = new MySqlBackup(command)) {
                        command.Connection = connection;
                        connection.Open();
                        sql = backup.ExportToString();
                        connection.Close();
                    }
                }
            }

            return sql;
        }

        private static async Task UploadSqlDumpToStorage(string planName, string sqlDump) {
            var storageConnectionString = _config.GetConnectionString("Storage");
            var containerName = _config["StorageContainerName"];
            var blobPrefix = _config["StorageBlobPrefix"];

            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            var cloudBlobClient = storageAccount.CreateCloudBlobClient();

            var cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            await cloudBlobContainer.CreateIfNotExistsAsync();

            var blobName = GenerateStorageBlobName(planName, blobPrefix);
            var blob = cloudBlobContainer.GetBlockBlobReference(blobName);

            await blob.UploadTextAsync(sqlDump);
        }

        private static string GenerateStorageBlobName(string databaseName, string blobPrefix) {
            var now = DateTime.Now;

            return $"{blobPrefix}/{databaseName}/{now.Year}_{now.Month}_{now.Day}_{now.Ticks}.sql";
        }

        private static Dictionary<string, string> GetDatabases() {
            var prefix = "ConnectionStrings:Database-";

            var connectionStrings = _config.AsEnumerable()
                .Where(o => o.Key.StartsWith(prefix))
                .ToDictionary(o => o.Key, o => o.Value);

            var databases = new Dictionary<string, string>();

            foreach (var item in connectionStrings) {
                var regex = new Regex($"{prefix}(.*)");
                var match = regex.Match(item.Key);

                if (match.Success) {
                    var planName = match.Groups[1].Value;
                    var connectionString = item.Value;

                    databases.Add(planName, connectionString);
                }
            }

            return databases;
        }
    }
}
