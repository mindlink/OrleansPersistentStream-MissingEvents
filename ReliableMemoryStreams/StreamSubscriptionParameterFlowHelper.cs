namespace OrleansMissingEvents.ReliableMemoryStreams
{
    using Orleans.Runtime;

    internal static class StreamSubscriptionParameterFlowHelper
    {
        private const string RootRequestContextKeyPrefix = "MindLink.ReliableMemoryStreamsStreamProviderName.";

        private const string CurrentSubscribingStreamIdRequestContextKey = $"{RootRequestContextKeyPrefix}CurrentSubscribingStreamId";

        private const string CurrentSubscribingSubscriptionIdRequestContextKey = $"{RootRequestContextKeyPrefix}CurrentSubscribingSubscriptionId";

        public static void SetFlowedStreamSubscriptionParameters(QualifiedStreamId streamId, GuidId subscriptionId)
        {
            RequestContext.Set(CurrentSubscribingStreamIdRequestContextKey, streamId);
            RequestContext.Set(CurrentSubscribingSubscriptionIdRequestContextKey, subscriptionId);
        }

        public static StreamSubscriptionParameterBundle? GetFlowedStreamSubscriptionParameters()
        {
            var streamId = RequestContext.Get(CurrentSubscribingStreamIdRequestContextKey) as QualifiedStreamId?;
            var subscriptionId = RequestContext.Get(CurrentSubscribingSubscriptionIdRequestContextKey) as GuidId;

            if (streamId == null || subscriptionId == null)
            {
                return null;
            }

            return new StreamSubscriptionParameterBundle(streamId.Value, subscriptionId);
        }

        public static void ClearFlowedStreamSubscriptionParameters()
        {
            RequestContext.Remove(CurrentSubscribingStreamIdRequestContextKey);
            RequestContext.Remove(CurrentSubscribingSubscriptionIdRequestContextKey);
        }
    }
}
