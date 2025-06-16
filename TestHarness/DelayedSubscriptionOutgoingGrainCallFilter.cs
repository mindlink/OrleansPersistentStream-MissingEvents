namespace OrleansMissingEvents.TestHarness
{
    internal class DelayedSubscriptionOutgoingGrainCallFilter : IOutgoingGrainCallFilter
    {
        /// <inheritdoc />
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            var isStreamingHandshake = context.InterfaceMethod.Name == "GetSequenceToken";

            if (context.InterfaceMethod.Name == "GetSequenceToken")
            {
                await Task.Delay(1000);
            }

            await context.Invoke();
        }
    }
}
