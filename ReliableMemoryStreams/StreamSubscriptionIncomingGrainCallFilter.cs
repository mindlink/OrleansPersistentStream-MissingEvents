namespace OrleansMissingEvents.ReliableMemoryStreams;

using Microsoft.Extensions.Logging;

internal sealed class StreamSubscriptionIncomingGrainCallFilter(
    IPendingStreamSubscriptionsCoordinationService pendingStreamSubscriptionsCoordinationService, ILogger<StreamSubscriptionIncomingGrainCallFilter> logger) : IIncomingGrainCallFilter
{
    private readonly IPendingStreamSubscriptionsCoordinationService pendingStreamSubscriptionsCoordinationService = pendingStreamSubscriptionsCoordinationService;

    private readonly ILogger logger = logger;

    public async Task Invoke(IIncomingGrainCallContext incomingGrainCallContext)
    {
        if (incomingGrainCallContext.InterfaceMethod.Name != "AddSubscriber" ||
            incomingGrainCallContext.InterfaceName != "Orleans.Streams.IStreamProducerExtension" ||
            incomingGrainCallContext.Request.GetArgumentCount() < 2)
        {
            await incomingGrainCallContext.Invoke();

            return;
        }

        var firstArgument = incomingGrainCallContext.Request.GetArgument(0);
        var secondArgument = incomingGrainCallContext.Request.GetArgument(1);

        if (firstArgument is not GuidId subscriptionId ||
            secondArgument is not QualifiedStreamId streamId ||
            !InternalOrleansStreamingReflectionHelper.TryGetPersistentStreamPullingAgentQueueId(incomingGrainCallContext.Grain, out var queueId))
        {
            this.logger.LogWarning(
                "Failed to handle stream subscription as the parameters could not be marshalled.");

            await incomingGrainCallContext.Invoke();

            return;
        }

        this.logger.LogDebug(
            "Handling stream subscription on queue '{0}' for stream '{1}' and subscription '{2}' by flowing parameters and registering pending stream subscription.",
            queueId,
            streamId,
            subscriptionId);

        StreamSubscriptionParameterFlowHelper.SetFlowedStreamSubscriptionParameters(streamId, subscriptionId);

        var pendingStreamSubscriptionsManager = pendingStreamSubscriptionsCoordinationService.GetPendingStreamSubscriptionsManagerForQueue(queueId);

        pendingStreamSubscriptionsManager.RegisterPendingStreamSubscription(streamId, subscriptionId);

        try
        {
            await incomingGrainCallContext.Invoke();
        }
        catch (Exception exception)
        {
            this.logger.LogDebug(
                "Handling failure of subscription on queue '{0}' for stream '{1}' and subscription '{2}' by flowing parameters and registering pending stream subscription: {3}",
                queueId,
                streamId,
                subscriptionId,
                exception);

            pendingStreamSubscriptionsManager.NotifyStreamSubscriptionFailed(streamId, subscriptionId);

            throw;
        }
        finally
        {
            StreamSubscriptionParameterFlowHelper.ClearFlowedStreamSubscriptionParameters();
        }
    }
}