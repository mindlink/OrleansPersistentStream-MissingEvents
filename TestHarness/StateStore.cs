namespace OrleansMissingEvents.TestHarness
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;

    internal class StateStore
    {
        private readonly ConcurrentDictionary<Guid, int> eventsByModelId = new ConcurrentDictionary<Guid, int>();

        public Task SetStateAsync(Guid modelId, int state)
        {
            eventsByModelId[modelId] = state;

            return Task.CompletedTask;
        }

        public int? GetState(Guid modelId)
        {
            if (eventsByModelId.TryGetValue(modelId, out var value))
            {
                return value;
            }

            return null;
        }

        public async Task<int?> GetStateAsync(Guid modelId)
        {
            // Simulate I/O to the database
            await Task.Delay(500);

            var state = GetState(modelId);

            // Simulate I/O from database
            await Task.Delay(500);

            return state;
        }
    }
}
