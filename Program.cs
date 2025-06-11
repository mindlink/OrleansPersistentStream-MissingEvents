using Microsoft.Extensions.Logging;
using Spectre.Console;

namespace OrleansMissingEvents
{
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;

    internal class Program
    {
        static async Task Main(string[] args)
        {
            AnsiConsole.Clear();

            var hostApplicationBuilder = Host.CreateApplicationBuilder();
                
            hostApplicationBuilder.UseOrleans(siloBuilder =>
            {
                siloBuilder
                    .UseLocalhostClustering()
                    .AddMemoryGrainStorage("PubSubStore")
                    .AddMemoryStreams("TestStream");
            });

            hostApplicationBuilder.Logging.ClearProviders();

            hostApplicationBuilder.Services.AddSingleton<StateStore>();
            
            var host = hostApplicationBuilder.Build();

            await host.StartAsync();

            var grainFactory = host.Services.GetRequiredService<IGrainFactory>();

            var producerId = Guid.NewGuid();

            var producerGrain = grainFactory.GetGrain<IProducerGrain>(producerId);
            var consumerGrain = grainFactory.GetGrain<IConsumerGrain>(Guid.NewGuid());

            await producerGrain.WakeUpStreamAsync(); // Ensure stream is initialized in PersistentStreamPullingAgent.

            await Task.Delay(500); // Wait for that initial event from the WakeUpStream call to settle.

            await consumerGrain.ExplicitSubscribe(producerId); // Subscribe to the stream.

            Console.ReadLine();
        }
    }
}
