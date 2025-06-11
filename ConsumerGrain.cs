using Microsoft.Extensions.Logging;
using Spectre.Console;

namespace OrleansMissingEvents
{
    using Orleans.Runtime;
    using Orleans.Streams;

    internal class ConsumerGrain(StateStore stateStore) : Grain, IConsumerGrain
    {
        private readonly StateStore stateStore = stateStore;

        public async Task ExplicitSubscribe(Guid modelId)
        {

            var handle = await this.GetStreamProvider("TestStream")
                .GetStream<int>(StreamId.Create("ns", modelId))
                .SubscribeAsync(this);

            AnsiConsole.MarkupLine("Subscribe complete.");

            var producer = this.GrainFactory.GetGrain<IProducerGrain>(modelId);

            Task.Run(async () =>
            {
                AnsiConsole.MarkupLine("Invoking producer.");

                await producer.MutateStateAsync();

                AnsiConsole.MarkupLine("Producer complete.");
            });

            AnsiConsole.MarkupLine("Getting state.");

            var state = await stateStore.GetStateAsync(modelId);

            AnsiConsole.MarkupLine("[green]Got initial state as: {0}[/]", state?.ToString() ?? "<none>");
        }

        public Task OnNextAsync(int item, StreamSequenceToken? token = null)
        {
            AnsiConsole.MarkupLine("[green]Received event: {0}[/]", item);

            return Task.CompletedTask;
        }

        public Task OnCompletedAsync()
        {
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            AnsiConsole.MarkupLine("[red]Got error:[/]");
            AnsiConsole.WriteException(ex);

            return Task.CompletedTask;
        }
    }
}
