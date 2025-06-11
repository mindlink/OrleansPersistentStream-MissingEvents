namespace OrleansMissingEvents
{
    using Orleans.Streams;

    internal interface IConsumerGrain : IGrainWithGuidKey, IAsyncObserver<int>
    {
        Task ExplicitSubscribe(Guid modelId);
    }
}
