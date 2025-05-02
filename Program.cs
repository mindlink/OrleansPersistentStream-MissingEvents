namespace OrleansMissingEvents
{
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;

    internal class Program
    {
        static async Task Main(string[] args)
        {
            var siloHostBuilder = new HostBuilder().UseOrleans(siloBuilder =>
            {
                siloBuilder
                    .UseLocalhostClustering()
                    .AddMemoryGrainStorage("PubSubStore")
                    .AddMemoryStreams("TestStream");
            });

            var host = siloHostBuilder.Build();

            await host.StartAsync();

            var grainFactory = host.Services.GetRequiredService<IGrainFactory>();

            var producerId = Guid.NewGuid();

            var producerGrain = grainFactory.GetGrain<IProducerGrain>(producerId);
            var consumerGrain = grainFactory.GetGrain<IConsumerGrain>(Guid.NewGuid());

            await producerGrain.WakeUpStream(); // Ensure stream is initialized in PersistentStreamPullingAgent.

            await Task.Delay(1000); // Wait for that initial event from the WakeUpStream call to settle.

            await consumerGrain.ExplicitSubscribe(producerId); // Subscribe to the stream.

            await producerGrain.EmitEventsAsync(); // Emit more events.

            Console.ReadLine();
        }
    }
}
