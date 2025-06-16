namespace OrleansMissingEvents.TestHarness
{
    using Orleans.Runtime;
    using Orleans.Streams;

    internal class ProducerGrain(StateStore stateStore, CommandLineInterface commandLineInterface) : Grain, IProducerGrain
    {
        private readonly CommandLineInterface commandLineInterface = commandLineInterface;

        private readonly StateStore stateStore = stateStore;

        private IAsyncStream<int>? stream;

        public override Task OnActivateAsync(CancellationToken cancellationToken)
        {
            stream = this.GetStreamProvider("TestStream")
                .GetStream<int>(StreamId.Create("ns", this.GetPrimaryKey()));

            return base.OnActivateAsync(cancellationToken);
        }

        public async Task WakeUpStreamAsync()
        {
            await stream!.OnNextAsync(-1); // Wake up the stream.
        }

        public async Task MutateStateAsync(int mutationEventCount)
        {
            if (mutationEventCount <= 0)
            {
                throw new ArgumentException("Mutation event count value must be greater than zero.", nameof(mutationEventCount));
            }

            var modelId = this.GetPrimaryKey();

            var state = stateStore.GetState(modelId);

            if (state != null)
            {
                throw new InvalidOperationException($"State has already been published for this producer as {state}.");
            }

            commandLineInterface.WriteProducerLogMessage("Mutating state from {0}.", state?.ToString() ?? "<none>");

            foreach (var i in Enumerable.Range(0, mutationEventCount))
            {
                commandLineInterface.WriteProducerLogMessage("Setting state as {0}.", i);

                await stateStore.SetStateAsync(modelId, i);

                commandLineInterface.WriteProducerLogMessage("Publishing mutation event as {0}.", i);

                await stream!.OnNextAsync(i);

                commandLineInterface.WriteProducerLogMessage("Awaiting after publication of mutation event as {0}.", i);

                await Task.Delay(50);
            }
        }
    }
}
