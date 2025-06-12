namespace OrleansMissingEvents
{
    using Orleans.Streams;

    internal interface IConsumerGrain : IGrainWithGuidKey, IAsyncObserver<int>
    {
        Task RunTestAsync(TestMode testMode, int mutationEventCount);
    }
}
