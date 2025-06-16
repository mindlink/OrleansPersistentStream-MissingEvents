namespace OrleansMissingEvents.ReliableSubscription
{
    public sealed class ReliableSubscriptionGrainExtension(IReliableSubscriptionManager reliableSubscriptionManager)
        : IReliableSubscriptionGrainExtension
    {
        private readonly IReliableSubscriptionManager reliableSubscriptionManager = reliableSubscriptionManager;

        public Task SignalSubscriptionHandshakeCompletedAsync(Guid subscriptionHandleId)
        {
            return reliableSubscriptionManager.SignalSubscriptionHandshakeCompletedAsync(subscriptionHandleId);
        }
    }
}
