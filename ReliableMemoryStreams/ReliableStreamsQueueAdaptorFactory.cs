namespace OrleansMissingEvents.ReliableMemoryStreams;

using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

internal sealed class ReliableStreamsQueueAdaptorFactory(
    IQueueAdapterFactory queueAdaptorFactory, 
    IPendingStreamSubscriptionsCoordinationService pendingStreamSubscriptionsCoordinationService,
    ILoggerFactory loggerFactory) : IQueueAdapterFactory
{
    private readonly IPendingStreamSubscriptionsCoordinationService pendingStreamSubscriptionsCoordinationService = pendingStreamSubscriptionsCoordinationService;

    private readonly IQueueAdapterFactory queueAdapterFactory = queueAdaptorFactory;

    private readonly ILoggerFactory loggerFactory = loggerFactory;

    public Task<IQueueAdapter> CreateAdapter()
    {
        return this.queueAdapterFactory.CreateAdapter();
    }

    public IQueueAdapterCache GetQueueAdapterCache()
    {
        var queueAdaptorCache = this.queueAdapterFactory.GetQueueAdapterCache();

        return new ReliableStreamsQueueAdaptorCache(
            queueAdaptorCache, this.pendingStreamSubscriptionsCoordinationService, this.loggerFactory);
    }

    public IStreamQueueMapper GetStreamQueueMapper()
    {
        return this.queueAdapterFactory.GetStreamQueueMapper();
    }

    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
    {
        return this.queueAdapterFactory.GetDeliveryFailureHandler(queueId);
    }
}