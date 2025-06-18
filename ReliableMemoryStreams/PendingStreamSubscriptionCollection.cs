namespace OrleansMissingEvents.ReliableMemoryStreams;

using System.Collections.Generic;

internal sealed class PendingStreamSubscriptionCollection
{
    private readonly Dictionary<GuidId, PinnedStreamSubscriptionData?> pendingPinnedStreamSubscriptionDatasById = new Dictionary<GuidId, PinnedStreamSubscriptionData?>();

    private readonly Dictionary<StreamId, HashSet<GuidId>> pendingStreamSubscriptionIdsByStreamId =
        new Dictionary<StreamId, HashSet<GuidId>>();

    public int StreamsWithPendingSubscriptionsCount => this.pendingStreamSubscriptionIdsByStreamId.Count;

    public int PendingSubscriptionsCount => this.pendingStreamSubscriptionIdsByStreamId.Count;

    public void AddPendingStreamSubscription(StreamId streamId, GuidId subscriptionId)
    {
        this.pendingPinnedStreamSubscriptionDatasById.Add(subscriptionId, null);

        if (!this.pendingStreamSubscriptionIdsByStreamId.ContainsKey(streamId))
        {
            this.pendingStreamSubscriptionIdsByStreamId.Add(streamId, []);
        }

        this.pendingStreamSubscriptionIdsByStreamId[streamId].Add(subscriptionId);
    }

    public IEnumerable<GuidId> GetPendingUnpinnedStreamSubscriptionIdsForStream(StreamId streamId)
    {
        if (!this.pendingStreamSubscriptionIdsByStreamId.TryGetValue(streamId, out var pendingSubscriptionIdsForStream))
        {
            return [];
        }

        return pendingSubscriptionIdsForStream
            .Where(subscriptionId => this.pendingPinnedStreamSubscriptionDatasById[subscriptionId] == null).ToList();
    }

    public void SetPinnedStreamSubscriptionData(GuidId subscriptionId, PinnedStreamSubscriptionData pinnedStreamSubscriptionData)
    {
        if (!this.pendingPinnedStreamSubscriptionDatasById.ContainsKey(subscriptionId))
        {
            throw new InvalidOperationException(
                $"Unable to set pinned stream subscription data for non-existent subscription ID '{subscriptionId}'.");
        }

        this.pendingPinnedStreamSubscriptionDatasById[subscriptionId] = pinnedStreamSubscriptionData;
    }

    public bool RemovePendingStreamSubscription(StreamId streamId, GuidId subscriptionId, out PinnedStreamSubscriptionData? pinnedSubscriptionData)
    {
        pinnedSubscriptionData = null;

        if (!this.pendingStreamSubscriptionIdsByStreamId.TryGetValue(streamId, out var pendingSubscriptionIdsForStream))
        {
            return false;
        }

        if (!pendingSubscriptionIdsForStream.Contains(subscriptionId))
        {
            return false;
        }

        pendingSubscriptionIdsForStream.Remove(subscriptionId);

        if (!pendingSubscriptionIdsForStream.Any())
        {
            this.pendingStreamSubscriptionIdsByStreamId.Remove(streamId);
        }

        return this.pendingPinnedStreamSubscriptionDatasById.Remove(subscriptionId, out pinnedSubscriptionData);
    }
}