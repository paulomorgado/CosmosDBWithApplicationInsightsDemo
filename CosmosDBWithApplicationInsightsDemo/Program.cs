using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.ApplicationInsights.WorkerService;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace Paulomorgado.CosmosDBWithApplicationInsightsDemo
{
    public  static class Program
    {
        public static async Task Main(string[] args)
        {
            //System.Console.Write($"{System.Diagnostics.Process.GetCurrentProcess().Id}: ");
            //System.Console.ReadLine();
            var host = CreateHostBuilder(args).Build();

            var applicationInsightsServiceOptions = host.Services.GetService<IOptions<ApplicationInsightsServiceOptions>>();

            if (!(applicationInsightsServiceOptions?.Value?.EnableAdaptiveSampling).GetValueOrDefault(true))
            {
                var fixedSamplingPercentage = host.Services.GetService<IConfiguration>().GetValue<double>("ApplicationInsights:FixedSamplingPercentage", 100.0);
                var telemetryConfiguration = host.Services.GetService<TelemetryConfiguration>();
                telemetryConfiguration.DefaultTelemetrySink.TelemetryProcessorChainBuilder.UseSampling(fixedSamplingPercentage);
            }

            var telemetryClient = host.Services.GetService<TelemetryClient>();

            await host.RunAsync();

            // Explicitly call Flush() followed by sleep is required in Console Apps.
            // This is to ensure that even if application terminates, telemetry is sent to the back-end.
            if (telemetryClient is not null)
            {
                telemetryClient.Flush();
                await Task.Delay(30000);
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    var cosmosConfiguration = hostContext.Configuration.GetSection("cosmosdb");
                    var cosmosConnectionString = cosmosConfiguration.GetSection("connectionString").Value;
                    var cosmosClientOptions = cosmosConfiguration.GetValue<CosmosClientOptions>("options");
                    services.AddSingleton<CosmosClient>(_ => new CosmosClient(cosmosConnectionString, cosmosClientOptions));

                    services.AddHostedService<Worker>();

                    services.AddApplicationInsightsTelemetryWorkerService();
                });
    }
}
