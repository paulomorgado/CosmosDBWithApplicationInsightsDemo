using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Paulomorgado.CosmosDBWithApplicationInsightsDemo
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> logger;
        private readonly TelemetryClient telemetryClient;
        private readonly CosmosClient cosmosClient;
        private readonly IHostApplicationLifetime hostApplicationLifetime;
        private Database database;
        private Container container;
        private const string databaseId = "db";
        private const string containerId = "items";

        public Worker(CosmosClient cosmosClient, IHostApplicationLifetime hostApplicationLifetime, ILogger<Worker> logger, TelemetryClient telemetryClient)
        {
            this.cosmosClient = cosmosClient;
            this.hostApplicationLifetime = hostApplicationLifetime;
            this.logger = logger;
            this.telemetryClient = telemetryClient;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            this.logger.LogExecuteStarting(DateTimeOffset.Now);

            using (var operation = this.telemetryClient.StartOperation<RequestTelemetry>(nameof(Worker)))
            {
                try
                {
                    await this.CreateDatabaseAsync();
                    await this.CreateContainerAsync();
                    await this.AddItemsToContainerAsync();
                    await this.QueryItemsAsync();
                    await this.ReplaceFamilyItemAsync();
                    await this.DeleteFamilyItemAsync();
                    await this.DeleteDatabaseAndCleanupAsync();
                }
                catch (Exception ex)
                {
                    this.telemetryClient.TrackException(ex);
                    this.logger.LogExecuteError(ex);
                }
            }

            this.logger.LogExecuteFinished(DateTimeOffset.Now);

            this.telemetryClient.Flush();

            this.hostApplicationLifetime.StopApplication();
        }
        private async Task CreateDatabaseAsync()
        {
            using (this.telemetryClient.StartOperation<RequestTelemetry>(nameof(CreateDatabaseAsync)))
            {
                using (var operation = this.telemetryClient.StartOperation<RequestTelemetry>($"{nameof(CreateDatabaseAsync)}.{nameof(cosmosClient.CreateDatabaseIfNotExistsAsync)}"))
                {
                    // Create a new database
                    var databaseResponse = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
                    this.database = databaseResponse.Database;

                    AddComosDiagnosticsToTelemetry(operation, databaseResponse);
                }

                this.logger.LogCreateDatabase(this.database.Id);
            }
        }

        private async Task CreateContainerAsync()
        {
            using (this.telemetryClient.StartOperation<RequestTelemetry>(nameof(CreateContainerAsync)))
            {
                using (var operation = this.telemetryClient.StartOperation<RequestTelemetry>($"{nameof(CreateContainerAsync)}.{nameof(this.database.CreateContainerIfNotExistsAsync)}"))
                {
                    // Create a new container
                    var containerResponse = await this.database.CreateContainerIfNotExistsAsync(containerId, "/LastName"/*, 400*/);
                    this.container = containerResponse;

                    AddComosDiagnosticsToTelemetry(operation, containerResponse);
                }

                this.logger.LogCreateContainer(this.container.Id);
            }
        }

        private async Task AddItemsToContainerAsync()
        {
            using (this.telemetryClient.StartOperation<RequestTelemetry>(nameof(AddItemsToContainerAsync)))
            {
                // Create a family object for the Andersen family
                var andersenFamily = new Family
                {
                    Id = "Andersen.1",
                    LastName = "Andersen",
                    Parents = new Parent[]
                    {
                    new Parent { FirstName = "Thomas" },
                    new Parent { FirstName = "Mary Kay" }
                    },
                    Children = new Child[]
                    {
                    new Child
                    {
                        FirstName = "Henriette Thaulow",
                        Gender = "female",
                        Grade = 5,
                        Pets = new Pet[]
                        {
                            new Pet { GivenName = "Fluffy" }
                        }
                    }
                    },
                    Address = new Address { State = "WA", County = "King", City = "Seattle" },
                    IsRegistered = false
                };

                try
                {
                    ItemResponse<Family> andersenFamilyResponse;
                    using (var operation1 = this.telemetryClient.StartOperation<RequestTelemetry>($"{nameof(AddItemsToContainerAsync)}.{nameof(this.container.ReadItemAsync)}"))
                    {
                        operation1.Telemetry.Properties.Add("FamilyMemeberId", andersenFamily.Id);
                        operation1.Telemetry.Properties.Add("PartitionKey", andersenFamily.LastName);

                        try
                        {
                            // Read the item to see if it exists.  
                            andersenFamilyResponse = await this.container.ReadItemAsync<Family>(andersenFamily.Id, new PartitionKey(andersenFamily.LastName));

                            AddComosDiagnosticsToTelemetry(operation1, andersenFamilyResponse);
                        }
                        catch (CosmosException ex)
                        {
                            AddComosDiagnosticsToTelemetry(operation1, ex.Diagnostics);
                            throw;
                        }
                    }

                    this.logger.LogItemAlreadyExists(andersenFamilyResponse.Resource.Id);
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    ItemResponse<Family> andersenFamilyResponse;
                    using (var operation2 = this.telemetryClient.StartOperation<RequestTelemetry>($"{nameof(AddItemsToContainerAsync)}.{nameof(this.container.CreateItemAsync)}"))
                    {
                        operation2.Telemetry.Properties.Add("FamilyMemeberId", andersenFamily.Id);
                        operation2.Telemetry.Properties.Add("PartitionKey", andersenFamily.LastName);

                        // Create an item in the container representing the Andersen family. Note we provide the value of the partition key for this item, which is "Andersen"
                        andersenFamilyResponse = await this.container.CreateItemAsync<Family>(andersenFamily, new PartitionKey(andersenFamily.LastName));

                        AddComosDiagnosticsToTelemetry(operation2, andersenFamilyResponse);
                    }

                    // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse. We can also access the RequestCharge property to see the amount of RUs consumed on this request.
                    this.logger.LogItemCreated(andersenFamilyResponse.Resource.Id, andersenFamilyResponse.RequestCharge);
                }

                // Create a family object for the Wakefield family
                var wakefieldFamily = new Family
                {
                    Id = "Wakefield.7",
                    LastName = "Wakefield",
                    Parents = new Parent[]
                    {
                    new Parent { FamilyName = "Wakefield", FirstName = "Robin" },
                    new Parent { FamilyName = "Miller", FirstName = "Ben" }
                    },
                    Children = new Child[]
                    {
                    new Child
                    {
                        FamilyName = "Merriam",
                        FirstName = "Jesse",
                        Gender = "female",
                        Grade = 8,
                        Pets = new Pet[]
                        {
                            new Pet { GivenName = "Goofy" },
                            new Pet { GivenName = "Shadow" }
                        }
                    },
                    new Child
                    {
                        FamilyName = "Miller",
                        FirstName = "Lisa",
                        Gender = "female",
                        Grade = 1
                    }
                    },
                    Address = new Address { State = "NY", County = "Manhattan", City = "NY" },
                    IsRegistered = true
                };

                try
                {
                    ItemResponse<Family> wakefieldFamilyResponse;
                    using (var operation3 = this.telemetryClient.StartOperation<RequestTelemetry>($"{nameof(AddItemsToContainerAsync)}.{nameof(this.container.ReadItemAsync)}"))
                    {
                        operation3.Telemetry.Properties.Add("FamilyMemeberId", wakefieldFamily.Id);
                        operation3.Telemetry.Properties.Add("PartitionKey", wakefieldFamily.LastName);

                        try
                        {
                            // Read the item to see if it exists
                            wakefieldFamilyResponse = await this.container.ReadItemAsync<Family>(wakefieldFamily.Id, new PartitionKey(wakefieldFamily.LastName));

                            AddComosDiagnosticsToTelemetry(operation3, wakefieldFamilyResponse);
                        }
                        catch (CosmosException ex)
                        {
                            AddComosDiagnosticsToTelemetry(operation3, ex.Diagnostics);
                            throw;
                        }
                    }

                    this.logger.LogItemAlreadyExists(wakefieldFamilyResponse.Resource.Id);
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    ItemResponse<Family> wakefieldFamilyResponse;
                    using (var operation4 = this.telemetryClient.StartOperation<RequestTelemetry>($"{nameof(AddItemsToContainerAsync)}.{nameof(this.container.CreateItemAsync)}"))
                    {
                        operation4.Telemetry.Properties.Add("FamilyMemeberId", wakefieldFamily.Id);
                        operation4.Telemetry.Properties.Add("PartitionKey", wakefieldFamily.LastName);

                        // Create an item in the container representing the Wakefield family. Note we provide the value of the partition key for this item, which is "Wakefield"
                        wakefieldFamilyResponse = await this.container.CreateItemAsync<Family>(wakefieldFamily, new PartitionKey(wakefieldFamily.LastName));

                        AddComosDiagnosticsToTelemetry(operation4, wakefieldFamilyResponse);
                    }

                    // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse. We can also access the RequestCharge property to see the amount of RUs consumed on this request.
                    this.logger.LogItemCreated(wakefieldFamilyResponse.Resource.Id, wakefieldFamilyResponse.RequestCharge);
                }
            }
        }

        private async Task QueryItemsAsync()
        {
            using (var operation = this.telemetryClient.StartOperation<RequestTelemetry>(nameof(QueryItemsAsync)))
            {
                var sqlQueryText = "SELECT * FROM c WHERE c.LastName = 'Andersen'";

                this.logger.LogRunningQuery(sqlQueryText);

                var queryDefinition = new QueryDefinition(sqlQueryText);

                FeedIterator<Family> queryResultSetIterator;
                using (var operation1 = this.telemetryClient.StartOperation<RequestTelemetry>($"{nameof(QueryItemsAsync)}.{nameof(this.container.GetItemQueryIterator)}"))
                {
                    operation1.Telemetry.Properties.Add("Query", sqlQueryText);

                    queryResultSetIterator = this.container.GetItemQueryIterator<Family>(queryDefinition);
                }

                var families = new List<Family>();

                while (queryResultSetIterator.HasMoreResults)
                {
                    FeedResponse<Family> currentResultSet;
                    using (var operation2 = this.telemetryClient.StartOperation<RequestTelemetry>($"{nameof(QueryItemsAsync)}.{nameof(queryResultSetIterator.ReadNextAsync)}"))
                    {
                        currentResultSet = await queryResultSetIterator.ReadNextAsync();

                        AddComosDiagnosticsToTelemetry(operation2, currentResultSet);
                    }

                    foreach (var family in currentResultSet)
                    {
                        families.Add(family);
                        this.logger.LogReadFamily(family);
                    }
                }
            }
        }

        private async Task ReplaceFamilyItemAsync()
        {
            using (var operation = this.telemetryClient.StartOperation<RequestTelemetry>(nameof(ReplaceFamilyItemAsync)))
            {
                ItemResponse<Family> wakefieldFamilyResponse;
                using (var operation1 = this.telemetryClient.StartOperation<RequestTelemetry>($"{nameof(ReplaceFamilyItemAsync)}.{nameof(this.container.ReadItemAsync)}"))
                {
                    operation1.Telemetry.Properties.Add("FamilyMemeberId", "Wakefield.7");
                    operation1.Telemetry.Properties.Add("PartitionKey", "Wakefield");

                    wakefieldFamilyResponse = await this.container.ReadItemAsync<Family>("Wakefield.7", new PartitionKey("Wakefield"));

                    AddComosDiagnosticsToTelemetry(operation1, wakefieldFamilyResponse);
                }

                var itemBody = wakefieldFamilyResponse.Resource;

                // update registration status from false to true
                itemBody.IsRegistered = true;
                // update grade of child
                itemBody.Children[0].Grade = 6;

                using (var operation2 = this.telemetryClient.StartOperation<RequestTelemetry>($"{nameof(ReplaceFamilyItemAsync)}.{nameof(this.container.ReplaceItemAsync)}"))
                {
                    operation2.Telemetry.Properties.Add("FamilyMemeberId", itemBody.Id);
                    operation2.Telemetry.Properties.Add("PartitionKey", itemBody.LastName);

                    // replace the item with the updated content
                    wakefieldFamilyResponse = await this.container.ReplaceItemAsync<Family>(itemBody, itemBody.Id, new PartitionKey(itemBody.LastName));

                    AddComosDiagnosticsToTelemetry(operation2, wakefieldFamilyResponse);
                }

                this.logger.LogUpdateFamily(itemBody.LastName, itemBody.Id, wakefieldFamilyResponse.Resource);
            }
        }

        private async Task DeleteFamilyItemAsync()
        {
            using (var operation = this.telemetryClient.StartOperation<RequestTelemetry>(nameof(DeleteFamilyItemAsync)))
            {
                var partitionKeyValue = "Wakefield";
                var familyId = "Wakefield.7";

                ItemResponse<Family> wakefieldFamilyResponse;
                using (var operation1 = this.telemetryClient.StartOperation<RequestTelemetry>($"{nameof(DeleteFamilyItemAsync)}.{nameof(this.container.DeleteItemAsync)}"))
                {
                    operation1.Telemetry.Properties.Add("FamilyMemeberId", familyId);
                    operation1.Telemetry.Properties.Add("PartitionKey", partitionKeyValue);

                    // Delete an item. Note we must provide the partition key value and id of the item to delete
                    wakefieldFamilyResponse = await this.container.DeleteItemAsync<Family>(familyId, new PartitionKey(partitionKeyValue));

                    AddComosDiagnosticsToTelemetry(operation1, wakefieldFamilyResponse);
                }


                this.logger.LogDeleteFamily(partitionKeyValue, familyId);
            }
        }

        private async Task DeleteDatabaseAndCleanupAsync()
        {
            using (this.telemetryClient.StartOperation<RequestTelemetry>(nameof(DeleteDatabaseAndCleanupAsync)))
            {
                using (var operation = this.telemetryClient.StartOperation<RequestTelemetry>($"{nameof(DeleteDatabaseAndCleanupAsync)}.{nameof(this.database.DeleteAsync)}"))
                {
                    var databaseResourceResponse = await this.database.DeleteAsync();

                    AddComosDiagnosticsToTelemetry(operation, databaseResourceResponse);
                }

                this.logger.LogDeleteDatabase(databaseId);
            }
        }

        private static void AddComosDiagnosticsToTelemetry<OperationTelemetry, TResponse>(IOperationHolder<OperationTelemetry> operation, Response<TResponse> response)
            where OperationTelemetry : Microsoft.ApplicationInsights.Extensibility.Implementation.OperationTelemetry
            => AddComosDiagnosticsToTelemetry(operation, response.Diagnostics);

        private static void AddComosDiagnosticsToTelemetry<OperationTelemetry>(IOperationHolder<OperationTelemetry> operation, CosmosDiagnostics cosmosDiagnostics)
            where OperationTelemetry : Microsoft.ApplicationInsights.Extensibility.Implementation.OperationTelemetry
            => operation.Telemetry.Properties.Add("CosmosDbDiagnostics", cosmosDiagnostics.ToString());
    }
}
