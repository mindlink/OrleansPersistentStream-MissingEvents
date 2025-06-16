namespace OrleansMissingEvents.ReliableSubscription
{
    internal interface IReliableSubscriptionGrainExtension : IGrainExtension
    {
        Task SignalSubscriptionHandshakeCompletedAsync(Guid subscriptionHandleId);
    }
}
