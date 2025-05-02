namespace OrleansMissingEvents
{
    internal interface IProducerGrain : IGrainWithGuidKey
    {
        Task WakeUpStream();
        Task EmitEventsAsync();
    }
}
