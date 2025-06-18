namespace OrleansMissingEvents.ReliableMemoryStreams;

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

internal sealed class PendingStreamSubscriptionsCoordinationService(ILoggerFactory loggerFactory) : IPendingStreamSubscriptionsCoordinationService
{
    private readonly ILoggerFactory loggerFactory = loggerFactory;

    private readonly ConcurrentDictionary<QueueId, PendingStreamSubscriptionsManager>
        pendingStreamSubscriptionsManagersByQueuedId = new ConcurrentDictionary<QueueId, PendingStreamSubscriptionsManager>();

    public PendingStreamSubscriptionsManager GetPendingStreamSubscriptionsManagerForQueue(QueueId queueId)
    {
        return pendingStreamSubscriptionsManagersByQueuedId.GetOrAdd(queueId, _ => new PendingStreamSubscriptionsManager(this.loggerFactory.CreateLogger<PendingStreamSubscriptionsManager>()));
    }
}