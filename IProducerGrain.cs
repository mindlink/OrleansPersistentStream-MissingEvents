namespace OrleansMissingEvents
{
    internal interface IProducerGrain : IGrainWithGuidKey
    {
        Task WakeUpStreamAsync();

        Task MutateStateAsync(int mutationEventCount);
    }
}
