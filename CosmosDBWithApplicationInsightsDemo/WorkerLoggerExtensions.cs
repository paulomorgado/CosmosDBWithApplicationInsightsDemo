using Microsoft.Extensions.Logging;
using System;

namespace Paulomorgado.CosmosDBWithApplicationInsightsDemo
{
    internal static class WorkerLoggerExtensions
    {
        private static readonly Action<ILogger, DateTimeOffset, Exception> logExecuteStartingAction = LoggerMessage.Define<DateTimeOffset>(LogLevel.Information, new EventId(1, "ExecuteStarting"), "Worker starting at: {time}");
        private static readonly Action<ILogger, DateTimeOffset, Exception> logExecuteFinishedAction = LoggerMessage.Define<DateTimeOffset>(LogLevel.Information, new EventId(2, "ExecuteFinished"), "Worker finished at: {time}");
        private static readonly Action<ILogger, Exception> logExecuteErrorAction = LoggerMessage.Define(LogLevel.Error, new EventId(3, "ExecuteError"), "Error running demo.");
        private static readonly Action<ILogger, string, Exception> logCreateDatabaseAction = LoggerMessage.Define<string>(LogLevel.Information, new EventId(1_001, "CreateDatabase"), "Created Database: {databaseId}.");
        private static readonly Action<ILogger, string, Exception> logDeleteDatabaseAction = LoggerMessage.Define<string>(LogLevel.Information, new EventId(1_002, "DeleteDatabase"), "Deleted Database: {databaseId}.");
        private static readonly Action<ILogger, string, Exception> logCreateContainerAction = LoggerMessage.Define<string>(LogLevel.Information, new EventId(2_001, "CreateContainer"), "Created Container: {containerId}.");
        private static readonly Action<ILogger, string, Exception> logItemAlreadyExistsAction = LoggerMessage.Define<string>(LogLevel.Information, new EventId(3_001, "ItemAlreadyExists"), "Item in database with id '{resourceId}' already exists.");
        private static readonly Action<ILogger, string, double, Exception> logItemCreatedAction = LoggerMessage.Define<string, double>(LogLevel.Information, new EventId(3_002, "ItemCreated"), "Created item in database with id '{resourceId}'. Operation consumed {charge} RUs");
        private static readonly Action<ILogger, string, Exception> logRunningQueryAction = LoggerMessage.Define<string>(LogLevel.Information, new EventId(4_001, "RunningQuery"), "Running query: '{sqlQueryText}'.");
        private static readonly Action<ILogger, Family, Exception> logReadFamilyAction = LoggerMessage.Define<Family>(LogLevel.Information, new EventId(4_002, "ReadFamily"), "Read '{family}'.");
        private static readonly Action<ILogger, string, string, Family, Exception> logUpdateFamilyAction = LoggerMessage.Define<string, string, Family>(LogLevel.Information, new EventId(5_001, "UpdateFamily"), "Updated Family [{lastName},{id}].\n \tBody is now: {family}.");
        private static readonly Action<ILogger, string, string, Exception> logDeleteFamilyAction = LoggerMessage.Define<string, string>(LogLevel.Information, new EventId(6_001, "DeleteFamily"), "Deleted Family [{partitionKeyValue},{resourceId}].");

        public static void LogExecuteStarting(this ILogger<Worker> logger, DateTimeOffset timestamp) => logExecuteStartingAction(logger, timestamp, null);
        public static void LogExecuteFinished(this ILogger<Worker> logger, DateTimeOffset timestamp) => logExecuteFinishedAction(logger, timestamp, null);
        public static void LogExecuteError(this ILogger<Worker> logger, Exception exception) => logExecuteErrorAction(logger, exception);

        public static void LogCreateDatabase(this ILogger<Worker> logger, string databaseId) => logCreateDatabaseAction(logger, databaseId, null);
        public static void LogDeleteDatabase(this ILogger<Worker> logger, string databaseId) => logDeleteDatabaseAction(logger, databaseId, null);

        public static void LogCreateContainer(this ILogger<Worker> logger, string containerId) => logCreateContainerAction(logger, containerId, null);

        public static void LogItemAlreadyExists(this ILogger<Worker> logger, string resourceId) => logItemAlreadyExistsAction(logger, resourceId, null);
        public static void LogItemCreated(this ILogger<Worker> logger, string resourceId, double charge) => logItemCreatedAction(logger, resourceId, charge, null);

        public static void LogRunningQuery(this ILogger<Worker> logger, string sqlQueryText) => logRunningQueryAction(logger, sqlQueryText, null);

        public static void LogReadFamily(this ILogger<Worker> logger, Family family) => logReadFamilyAction(logger, family, null);

        public static void LogUpdateFamily(this ILogger<Worker> logger, string lastName, string resourceId, Family family) => logUpdateFamilyAction(logger, lastName, resourceId, family, null);

        public static void LogDeleteFamily(this ILogger<Worker> logger, string partitionKeyValue, string resourceId) => logDeleteFamilyAction(logger, partitionKeyValue, resourceId, null);
    }
}
