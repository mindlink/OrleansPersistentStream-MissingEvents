namespace OrleansMissingEvents.ReliableMemoryStreams;

using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;

internal sealed class ReliableStreamsQueueCache(IQueueCache queueCache, PendingStreamSubscriptionsManager pendingSubscriptionsManager, ILogger<ReliableStreamsQueueCache> logger) : IQueueCache
{
    private readonly IQueueCache queueCache = queueCache;

    private readonly PendingStreamSubscriptionsManager pendingSubscriptionsManager = pendingSubscriptionsManager;

    private readonly ILogger<ReliableStreamsQueueCache> logger = logger;

    public int GetMaxAddCount()
    {
        return this.queueCache.GetMaxAddCount();
    }

    public void AddToCache(IList<IBatchContainer> batchContainers)
    {
        this.logger.LogDebug("Beginning adding {0} items to queue cache.", batchContainers.Count);

        this.queueCache.AddToCache(batchContainers);

        foreach (var batchContainer in batchContainers)
        {
            var streamId = batchContainer.StreamId;
            var firstItemStreamSequenceToken = batchContainer.SequenceToken;

            this.logger.LogDebug(
                "Notifying of addition of new queue cache items for stream '{0}' with stream sequence token of first item '{1}'.",
                streamId,
                firstItemStreamSequenceToken);

            this.pendingSubscriptionsManager.HandleNewCachedStreamItems(
                streamId,
                firstItemStreamSequenceToken,
                static state => state.queueCache.GetCacheCursor(state.streamId, state.firstItemStreamSequenceToken),
                (this.queueCache, streamId, firstItemStreamSequenceToken));
        }
    }

    public bool TryPurgeFromCache(out IList<IBatchContainer> purgedItems)
    {
        this.logger.LogTrace("Beginning trying to purge queue cache items by forwarding to inner queue cache.");

        return this.queueCache.TryPurgeFromCache(out purgedItems);
    }

    public IQueueCacheCursor GetCacheCursor(StreamId streamId, StreamSequenceToken? streamSequenceToken)
    {
        this.logger.LogDebug(
            "Beginning retrieving cache cursor for stream '{0}' with stream sequence token '{1}' by retrieving flowed subscription parameters.",
            streamId,
            streamSequenceToken?.ToString() ?? "<none>");

        var streamSubscriptionParameterBundle = StreamSubscriptionParameterFlowHelper.GetFlowedStreamSubscriptionParameters();

        if (streamSubscriptionParameterBundle == null)
        {
            this.logger.LogDebug(
                "Abandoning request to retrieve cache cursor for stream '{0}' with stream sequence token '{1}' by deferring to inner queue cache as no flowed subscription parameters could be obtained.",
                streamId,
                streamSequenceToken?.ToString() ?? "<none>");

            return this.queueCache.GetCacheCursor(streamId, streamSequenceToken);
        }

        var pendingStreamSubscriptionId = streamSubscriptionParameterBundle.SubscriptionId;

        this.logger.LogDebug(
            "Handling request to retrieve cache cursor for stream '{0}' with stream sequence token '{1}' for flowed subscription Id '{2}' by notifying of pending stream subscription completion.",
            streamId,
            streamSequenceToken?.ToString() ?? "<none>",
            pendingStreamSubscriptionId);

        var pinnedStreamSequenceToken = this.pendingSubscriptionsManager.NotifyStreamSubscriptionCompleted(streamId, streamSubscriptionParameterBundle.SubscriptionId);

        var effectiveStreamSequenceToken = pinnedStreamSequenceToken ?? streamSequenceToken;

        this.logger.LogDebug(
            "Completing request to retrieve cache cursor for stream '{0}' by deferring to inner queue cache with resolved effective stream sequence token '{1}'.",
            streamId,
            effectiveStreamSequenceToken);

        return this.queueCache.GetCacheCursor(streamId, effectiveStreamSequenceToken);
    }

    public bool IsUnderPressure()
    {
        return this.queueCache.IsUnderPressure();
    }
}