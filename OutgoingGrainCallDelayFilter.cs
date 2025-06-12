namespace OrleansMissingEvents
{
    /// <summary>
    /// Does a thing.
    /// </summary>
    internal class OutgoingGrainCallDelayFilter : IOutgoingGrainCallFilter
    {
        /// <inheritdoc />
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.InterfaceMethod.Name == "GetSequenceToken")
            {
                await Task.Delay(1000);
            }

            await context.Invoke();
        }
    }
}
