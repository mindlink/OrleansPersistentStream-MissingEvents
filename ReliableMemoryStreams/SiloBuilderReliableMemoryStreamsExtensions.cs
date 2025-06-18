namespace OrleansMissingEvents.ReliableMemoryStreams;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;

using System;
using Orleans.Providers;

internal static class SiloBuilderReliableMemoryStreamsExtensions
{
    public static ISiloBuilder AddReliableMemoryStreams(this ISiloBuilder siloBuilder, string name,
        Action<ISiloMemoryStreamConfigurator>? configure = null)
    {
        siloBuilder.AddPersistentStreams(
            name,
            (services, _) =>
            {
                var cachePurgeOptions = services.GetOptionsByName<StreamCacheEvictionOptions>(name);
                var statisticOptions = services.GetOptionsByName<StreamStatisticOptions>(name);
                var queueMapperOptions = services.GetOptionsByName<HashRingStreamQueueMapperOptions>(name);

                var grainFactory = services.GetRequiredService<IGrainFactory>();
                var loggerFactory = services.GetRequiredService<ILoggerFactory>();
                var pendingSubscriptionsCoordinationService =
                    services.GetRequiredService<IPendingStreamSubscriptionsCoordinationService>();

                var memoryAdaptorFactory = new MemoryAdapterFactory<DefaultMemoryMessageBodySerializer>(name,
                    cachePurgeOptions, statisticOptions,
                    queueMapperOptions, services, grainFactory, loggerFactory);

                memoryAdaptorFactory.Init();

                return new ReliableStreamsQueueAdaptorFactory(memoryAdaptorFactory,
                    pendingSubscriptionsCoordinationService, loggerFactory);
            },
            null);

        siloBuilder
            .AddOutgoingGrainCallFilter<ConsumerHandshakeOutgoingGrainCallFilter>()
            .AddIncomingGrainCallFilter<StreamSubscriptionIncomingGrainCallFilter>();

        siloBuilder.Services.AddSingleton<IPendingStreamSubscriptionsCoordinationService, PendingStreamSubscriptionsCoordinationService>();

        siloBuilder.Services
            .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name)
            .ConfigureNamedOptionForLogging<StreamStatisticOptions>(name)
            .ConfigureNamedOptionForLogging<StreamCacheEvictionOptions>(name);

        return siloBuilder;
    }
}