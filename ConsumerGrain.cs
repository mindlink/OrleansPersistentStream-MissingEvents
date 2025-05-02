namespace OrleansMissingEvents
{
    using Orleans.Runtime;
    using Orleans.Streams;

    internal class ConsumerGrain : Grain, IConsumerGrain
    {
        public async Task ExplicitSubscribe(Guid modelId)
        {
            var self = this.AsReference<IConsumerGrain>();

            await Task.Run(() =>
            {
                self.DoWork();
            });

            await this.GetStreamProvider("TestStream")
                .GetStream<int>(StreamId.Create("ns", modelId))
                .SubscribeAsync(this);

            Console.WriteLine("Subscribe Complete");
        }

        public async Task DoWork()
        {
            await Task.Delay(300);
        }

        public Task OnNextAsync(int item, StreamSequenceToken? token = null)
        {
            Console.WriteLine($"Received event: {item}.");

            return Task.CompletedTask;
        }

        public Task OnCompletedAsync()
        {
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            Console.WriteLine("Got error");

            return Task.CompletedTask;
        }
    }
}
