namespace OrleansMissingEvents.TestHarness;

using Orleans.Streams;

internal interface IConsumerGrain : IGrainWithGuidKey, IAsyncObserver<int>
{
    Task RunTestAsync(string streamProviderName, int mutationEventCount);
}