namespace OrleansMissingEvents.ReliableMemoryStreams;

using Microsoft.Extensions.Logging;
using Orleans.Streams;

internal sealed class ReliableStreamsQueueAdaptorCache(
    IQueueAdapterCache queueAdapterCache,
    IPendingStreamSubscriptionsCoordinationService pendingStreamSubscriptionsCoordinationService,
    ILoggerFactory loggerFactory) : IQueueAdapterCache
{
    private readonly IQueueAdapterCache queueAdapterCache = queueAdapterCache;

    private readonly IPendingStreamSubscriptionsCoordinationService pendingStreamSubscriptionsCoordinationService = pendingStreamSubscriptionsCoordinationService;

    private readonly ILoggerFactory loggerFactory = loggerFactory;

    public IQueueCache CreateQueueCache(QueueId queueId)
    {
        var queueCache = this.queueAdapterCache.CreateQueueCache(queueId);

        var reliableStreamsQueueCache = new ReliableStreamsQueueCache(
            queueCache, this.pendingStreamSubscriptionsCoordinationService.GetPendingStreamSubscriptionsManagerForQueue(queueId),
            this.loggerFactory.CreateLogger<ReliableStreamsQueueCache>());

        return reliableStreamsQueueCache;
    }
}