namespace OrleansMissingEvents.ReliableMemoryStreams;

using Orleans.Streams;

internal record PinnedStreamSubscriptionData(
    IQueueCacheCursor QueueCacheCursor,
    StreamSequenceToken FirstItemStreamSequenceToken);