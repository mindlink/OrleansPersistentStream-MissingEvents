namespace OrleansMissingEvents.ReliableMemoryStreams;

using System.Reflection;
using Orleans.Streams;

internal static class InternalOrleansStreamingReflectionHelper
{
    private static readonly FieldInfo PersistentStreamPullingAgentQueueIdFieldInfo;

    static InternalOrleansStreamingReflectionHelper()
    {
        var orleansStreamingAssembly = Assembly.Load("Orleans.Streaming");

        var persistentStreamPullingAgentType =
            orleansStreamingAssembly.GetType("Orleans.Streams.PersistentStreamPullingAgent");

        if (persistentStreamPullingAgentType == null)
        {
            throw new InvalidOperationException("Failed to load type information for persistent stream pulling agent.");
        }

        var persistentStreamPullingAgentQueueIdFieldInfo = persistentStreamPullingAgentType.GetField(
            "QueueId", BindingFlags.Instance | BindingFlags.NonPublic);

        if (persistentStreamPullingAgentQueueIdFieldInfo == null)
        {
            throw new InvalidOperationException("Failed to load field information for persistent stream pulling agent queue ID.");
        }

        PersistentStreamPullingAgentQueueIdFieldInfo = persistentStreamPullingAgentQueueIdFieldInfo;
    }

    public static bool TryGetPersistentStreamPullingAgentQueueId(object persistentStreamPullingAgent, out QueueId queueId)
    {
        var queueIdValue = PersistentStreamPullingAgentQueueIdFieldInfo.GetValue(persistentStreamPullingAgent);

        if (queueIdValue is not QueueId castQueueId)
        {
            queueId = default;
            return false;
        }

        queueId = castQueueId;
        return true;
    }
}