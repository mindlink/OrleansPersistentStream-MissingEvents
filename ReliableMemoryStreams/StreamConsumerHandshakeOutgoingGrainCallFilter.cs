namespace OrleansMissingEvents.ReliableMemoryStreams;

using Microsoft.Extensions.Logging;

internal sealed class ConsumerHandshakeOutgoingGrainCallFilter(
    IPendingStreamSubscriptionsCoordinationService pendingStreamSubscriptionsStorageService, ILogger<ConsumerHandshakeOutgoingGrainCallFilter> logger) : IOutgoingGrainCallFilter
{
    private readonly ILogger logger = logger;

    private readonly IPendingStreamSubscriptionsCoordinationService pendingStreamSubscriptionsStorageService = pendingStreamSubscriptionsStorageService;

    public async Task Invoke(IOutgoingGrainCallContext outgoingGrainCallContext)
    {
        if (outgoingGrainCallContext.InterfaceMethod.Name != "GetSequenceToken" ||
            outgoingGrainCallContext.InterfaceName != "Orleans.Streams.IStreamConsumerExtension")
        {
            await outgoingGrainCallContext.Invoke();

            return;
        }

        try
        {
            await outgoingGrainCallContext.Invoke();
        }
        catch (Exception exception)
        {
            this.logger.LogDebug(
                "Beginning handling stream consumer handshake exception {0}.", exception);

            var sourceContext = outgoingGrainCallContext.SourceContext!;

            if (!InternalOrleansStreamingReflectionHelper.TryGetPersistentStreamPullingAgentQueueId(sourceContext, out var queueId))
            {
                logger.LogWarning(
                    "Failed to handle stream consumer handshake exception as the queue ID could not be retrieved.");

                return;
            }

            var streamSubscriptionData = StreamSubscriptionParameterFlowHelper.GetFlowedStreamSubscriptionParameters();

            if (streamSubscriptionData == null)
            {
                logger.LogWarning(
                    "Failed to handle stream consumer handshake exception as no flowed subscription parameters could be retrieved.");

                return;
            }

            var pendingStreamSubscriptionsManager = this.pendingStreamSubscriptionsStorageService.GetPendingStreamSubscriptionsManagerForQueue(queueId);

            logger.LogDebug(
                "Notifying of stream subscription failure on queue '{0}' for stream '{1}' and subscription '{2}'.", queueId, streamSubscriptionData.StreamId, streamSubscriptionData.SubscriptionId);

            pendingStreamSubscriptionsManager.NotifyStreamSubscriptionFailed(streamSubscriptionData.StreamId, streamSubscriptionData.SubscriptionId);

            throw;
        }
    }
}