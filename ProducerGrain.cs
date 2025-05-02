namespace OrleansMissingEvents
{
    using Orleans.Runtime;
    using Orleans.Streams;

    internal class ProducerGrain : Grain, IProducerGrain
    {
        private IAsyncStream<int> stream;

        public override Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.stream = this.GetStreamProvider("TestStream")
                .GetStream<int>(StreamId.Create("ns", this.GetPrimaryKey()));

            return base.OnActivateAsync(cancellationToken);
        }

        public async Task WakeUpStream()
        {
            await this.stream.OnNextAsync(-1); // Wake up the stream.
        }

        public async Task EmitEventsAsync()
        {
            Console.WriteLine("Emitting events...");

            foreach (var i in Enumerable.Range(1, 10))
            {
                await this.stream.OnNextAsync(i);

                await Task.Delay(50);
            }
        }
    }
}
