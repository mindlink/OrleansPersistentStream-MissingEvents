using System.Collections.Concurrent;

namespace OrleansMissingEvents
{
    using System;
    using System.Threading.Tasks;

    internal class StateStore
    {
        private readonly ConcurrentDictionary<Guid, int> eventsByModelId = new ConcurrentDictionary<Guid, int>();

        public Task SetStateAsync(Guid modelId, int state)
        {
            this.eventsByModelId[modelId] = state;

            return Task.CompletedTask;
        }

        public int? GetState(Guid modelId)
        {
            if (this.eventsByModelId.TryGetValue(modelId, out var value))
            {
                return value;
            }

            return null;
        }

        public async Task<int?> GetStateAsync(Guid modelId)
        {
            await Task.Delay(500);

            var state = this.GetState(modelId);

            await Task.Delay(500);

            return state;
        }
    }
}
