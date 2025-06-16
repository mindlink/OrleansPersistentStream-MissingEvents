namespace OrleansMissingEvents.ReliableSubscription
{
    public interface IReliableSubscriptionGrainExtension : IGrainExtension
    {
        Task SignalSubscriptionHandshakeCompletedAsync(Guid subscriptionHandleId);
    }
}
