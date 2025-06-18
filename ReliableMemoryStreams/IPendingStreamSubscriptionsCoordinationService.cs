namespace OrleansMissingEvents.ReliableMemoryStreams;

using Orleans.Streams;

internal interface IPendingStreamSubscriptionsCoordinationService
{
    PendingStreamSubscriptionsManager GetPendingStreamSubscriptionsManagerForQueue(QueueId queueId);
}