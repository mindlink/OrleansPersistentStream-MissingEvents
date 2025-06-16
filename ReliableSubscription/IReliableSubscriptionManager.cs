namespace OrleansMissingEvents.ReliableSubscription
{
    using Orleans.Streams;

    internal interface IReliableSubscriptionManager
    {
        Task SubscribeReliablyAsync<TItems>(
            IAsyncObservable<TItems> asyncObservable,
            IAsyncObserver<TItems> asyncObserver,
            Func<object, Task> continuationFuncAsync,
            object state);

        Task SignalSubscriptionHandshakeCompletedAsync(Guid subscriptionHandleId);
    }
}
