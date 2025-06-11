namespace OrleansMissingEvents
{
    using Orleans.Runtime;
    using Orleans.Streams;
    using Spectre.Console;

    internal class ProducerGrain(StateStore stateStore) : Grain, IProducerGrain
    {
        private readonly StateStore stateStore = stateStore;

        private IAsyncStream<int>? stream;

        public override Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.stream = this.GetStreamProvider("TestStream")
                .GetStream<int>(StreamId.Create("ns", this.GetPrimaryKey()));

            return base.OnActivateAsync(cancellationToken);
        }

        public async Task WakeUpStreamAsync()
        {
            await this.stream.OnNextAsync(-1); // Wake up the stream.
        }

        public async Task MutateStateAsync()
        {
            var modelId = this.GetPrimaryKey();

            var state = stateStore.GetState(this.GetPrimaryKey());

            var beginningState = state ?? 0;

            AnsiConsole.MarkupLine("Mutating state from {0}", state?.ToString() ?? "<none>");

            foreach (var i in Enumerable.Range(beginningState, beginningState + 20))
            {
                AnsiConsole.MarkupLine("Setting state as {0}", i);

                await this.stateStore.SetStateAsync(modelId, i);

                AnsiConsole.MarkupLine("Publishing mutation event as {0}", i);

                await this.stream.OnNextAsync(i);

                AnsiConsole.MarkupLine("Awaiting after publication of mutation event as {0}", i);

                await Task.Delay(50);
            }
        }
    }
}
