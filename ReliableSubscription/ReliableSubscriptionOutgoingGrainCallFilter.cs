namespace OrleansMissingEvents.ReliableSubscription
{
    internal class ReliableSubscriptionOutgoingGrainCallFilter(IGrainFactory grainFactory) : IOutgoingGrainCallFilter
    {
        private readonly IGrainFactory grainFactory = grainFactory;

        /// <inheritdoc />
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            await context.Invoke();

            if (context.InterfaceMethod.Name == "GetSequenceToken")
            {
                var subscriptionHandleId = context.Request.GetArgument(0) as GuidId;

                context.SourceContext.Scheduler.QueueAction(() =>
                {
                    var grain = grainFactory.GetGrain<IReliableSubscriptionGrainExtension>(context.TargetId);

                    grain.SignalSubscriptionHandshakeCompletedAsync(subscriptionHandleId!.Guid);
                });
            }
        }
    }
}
