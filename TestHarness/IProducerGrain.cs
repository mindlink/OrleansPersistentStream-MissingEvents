namespace OrleansMissingEvents.TestHarness;

internal interface IProducerGrain : IGrainWithGuidKey
{
    Task WakeUpStreamAsync(string streamProviderName);

    Task MutateStateAsync(int mutationEventCount);
}