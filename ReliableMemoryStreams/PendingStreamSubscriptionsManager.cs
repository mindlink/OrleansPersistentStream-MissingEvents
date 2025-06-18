namespace OrleansMissingEvents.ReliableMemoryStreams;

using Microsoft.Extensions.Logging;
using Orleans.Streams;

internal sealed class PendingStreamSubscriptionsManager(ILogger<PendingStreamSubscriptionsManager> logger)
{
    private readonly ILogger<PendingStreamSubscriptionsManager> logger = logger;

    private readonly PendingStreamSubscriptionCollection pendingStreamSubscriptionCollection = new PendingStreamSubscriptionCollection();

    public void RegisterPendingStreamSubscription(StreamId streamId, GuidId subscriptionId)
    {
        this.pendingStreamSubscriptionCollection.AddPendingStreamSubscription(streamId, subscriptionId);
    }

    public void HandleNewCachedStreamItems<TState>(
        StreamId streamId, StreamSequenceToken firstItemStreamSequenceToken, Func<TState, IQueueCacheCursor> createQueueCacheCursorFunc, TState state)
    {
        var pendingUnpinnedStreamSubscriptionIds =
            this.pendingStreamSubscriptionCollection.GetPendingUnpinnedStreamSubscriptionIdsForStream(streamId);

        var pendingStreamSubscriptionsCount = this.pendingStreamSubscriptionCollection.PendingSubscriptionsCount;
        var streamsWithPendingSubscriptionsCount =
            this.pendingStreamSubscriptionCollection.StreamsWithPendingSubscriptionsCount;

        if (!pendingUnpinnedStreamSubscriptionIds.Any())
        {
            logger.LogDebug(
                "Abandoning handling new cached items for stream '{0}' with first item stream sequence token '{1}' as no pending and unpinned stream subscriptions were identified. There are '{2}' pending stream subscriptions across '{3}' streams.", streamId, firstItemStreamSequenceToken, pendingStreamSubscriptionsCount, streamsWithPendingSubscriptionsCount);

            return;
        }

        logger.LogDebug(
            "Handling new cached items for stream '{0}' with first item stream sequence token '{1}' by creating pinned queue cache cursor for '{2}' stream subscriptions. There are '{3}' pending stream subscriptions across '{4}' streams.", streamId, firstItemStreamSequenceToken, pendingUnpinnedStreamSubscriptionIds.Count(), pendingStreamSubscriptionsCount, streamsWithPendingSubscriptionsCount);

        foreach (var pendingUnpinnedSubscriptionId in pendingUnpinnedStreamSubscriptionIds)
        {
            var queueCacheCursor = createQueueCacheCursorFunc(state);

            logger.LogDebug(
                "Setting pinned stream subscription data for stream subscription '{0}' on stream '{1}' as queue cache cursor '{2}' for stream sequence token '{3}'.", pendingUnpinnedSubscriptionId, streamId, queueCacheCursor, firstItemStreamSequenceToken);

            this.pendingStreamSubscriptionCollection.SetPinnedStreamSubscriptionData(
                pendingUnpinnedSubscriptionId,
                new PinnedStreamSubscriptionData(queueCacheCursor, firstItemStreamSequenceToken));
        }
    }

    public StreamSequenceToken? NotifyStreamSubscriptionCompleted(
        StreamId streamId, GuidId subscriptionId)
    {
        return this.RemoveStreamSubscription("completion", streamId, subscriptionId);
    }

    public void NotifyStreamSubscriptionFailed(StreamId streamId, GuidId subscriptionId)
    {
        this.RemoveStreamSubscription("failure", streamId, subscriptionId);
    }

    private StreamSequenceToken? RemoveStreamSubscription(
        string operationDescription,
        StreamId streamId,
        GuidId subscriptionId)
    {
        var pendingSubscriptionsCount = this.pendingStreamSubscriptionCollection.PendingSubscriptionsCount;
        var streamsWithPendingSubscriptionsCount =
            this.pendingStreamSubscriptionCollection.StreamsWithPendingSubscriptionsCount;

        if (!this.pendingStreamSubscriptionCollection.RemovePendingStreamSubscription(streamId, subscriptionId, out var pinnedSubscriptionData))
        {
            logger.LogDebug(
                "Abandoning handling notification of {0} of stream subscription '{1}' for stream '{2}' as no pending stream subscription was identified. There are '{3}' pending stream subscriptions across '{4}' streams.",
                operationDescription,
                subscriptionId,
                streamId,
                pendingSubscriptionsCount,
                streamsWithPendingSubscriptionsCount);

            return null;
        }

        pendingSubscriptionsCount = this.pendingStreamSubscriptionCollection.PendingSubscriptionsCount;
        streamsWithPendingSubscriptionsCount =
            this.pendingStreamSubscriptionCollection.StreamsWithPendingSubscriptionsCount;

        if (pinnedSubscriptionData == null)
        {
            logger.LogDebug(
                "Abandoning handling notification of {0} of stream subscription '{1}' for stream '{2}' as a pending stream subscription was identified and removed but no pinned consumer data was registered. There are '{3}' pending stream subscriptions across '{4}' streams.",
                operationDescription,
                subscriptionId,
                streamId,
                pendingSubscriptionsCount,
                streamsWithPendingSubscriptionsCount);

            return null;
        }

        var queueCacheCursor = pinnedSubscriptionData.QueueCacheCursor;

        var firstItemStreamSequenceToken = pinnedSubscriptionData.FirstItemStreamSequenceToken;

        logger.LogDebug(
            "Handling notification of {0} of stream subscription '{1}' for stream '{2}' by disposing pinned queue cache cursor '{3}' and returning first item stream sequence token '{4}'. There are now '{5}' pending stream subscriptions across '{5}' streams.",
            operationDescription,
            subscriptionId,
            streamId,
            queueCacheCursor,
            firstItemStreamSequenceToken,
            pendingSubscriptionsCount,
            streamsWithPendingSubscriptionsCount);

        queueCacheCursor.Dispose();

        return firstItemStreamSequenceToken;
    }
}