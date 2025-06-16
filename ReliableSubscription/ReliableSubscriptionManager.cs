namespace OrleansMissingEvents.ReliableSubscription
{
    using Orleans.Streams;

    public sealed class ReliableSubscriptionManager : IReliableSubscriptionManager
    {
        private readonly Dictionary<Guid, ValueTuple<Func<object, Task>, object>>
            subscriptionContinuationDataBySubscriptionHandleId = new Dictionary<Guid, ValueTuple<Func<object, Task>, object>>();

        public async Task SubscribeReliablyAsync<TItems>(
            IAsyncObservable<TItems> asyncObservable,
            IAsyncObserver<TItems> asyncObserver,
            Func<object, Task> continuationFuncAsync,
            object state)
        {
            var subscriptionHandle = await asyncObservable.SubscribeAsync(asyncObserver);

            var subscriptionHandleId = subscriptionHandle.HandleId;

            subscriptionContinuationDataBySubscriptionHandleId.TryAdd(
                subscriptionHandleId, (continuationFuncAsync, state));
        }

        public Task SignalSubscriptionHandshakeCompletedAsync(Guid subscriptionHandleId)
        {
            if (!subscriptionContinuationDataBySubscriptionHandleId.TryGetValue(
                subscriptionHandleId, out var subscriptionContinuationData))
            {
                return Task.CompletedTask;
            }

            var (continuationFuncAsync, state) = subscriptionContinuationData;

            return continuationFuncAsync(state);
        }
    }
}
