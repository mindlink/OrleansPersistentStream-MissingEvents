# Overview
When the PersistentStreamPullingAgent already has an active stream and a new explicit subscription is setup by a Consumer, if the handshake takes a bit of time then events can appear to go missing from the Consumer's point of view after the SubscribeAsync finishes on the StreamProvider.

## The Problem
When the Consumer subscribes, the call goes via a PubSubGrain to the PersistentStreamPullingAgent. It kicks off some asynchronous work to perform the Consumer handshake and get the initial stream token if the Consumer specified one. The Task is rightly not awaited here (because of potential deadlocks) but it means that the Consumer's call to the SubscribeAsync on the StreamProvider completes before the PersistentStreamPullingAgent has actually created a cursor for the Consumer.

If the handshake takes a bit of time because it's waiting for the Consumer grain to do more work after successfully subscribing and any new events occur on the stream at the same time, the agent can process them all and evict them from the cache before the handshake completes, so the Consumer never gets them.

From the Consumer's point of view it looks like the events are missing.

This is problematic in our case because our real Consumer grains fetch the versioned state of the model from the DB after they've subscribed to their event stream to get up to date. The Producer models update their state in the DB and then emit events to notify Consumers. We rely on the fact that we can coordinate subscribing _and then_ fetching the state to ensure that no events can be missed in the interim.

We've used MemoryStreams here to show the issue, but in our actual code we've written a custom stream provider which uses a SimpleQueueCache. It's not rewindable so we don't use Consumer stream tokens on Subscribe at all.

## Repro
This project uses two grains to re-create the race condition described above:

1. `ProducerGrain`
2. `ConsumerGrain`

The `ConsumerGrain` gets told to subscribe to the `ProducerGrain` and then performs some busy-work immediately after (to simulate going off to the DB to fetch state about the Producer). This ensures that the handshake takes time because it's waiting at the grain boundary.

`ProducerGrain` is told to emit events 1-10 in order, `ConsumerGrain` writes out to the console as it sees each event.

### Expected Output:
```
Subscribe Complete
Emitting events...
Received event: 1.
Received event: 2.
Received event: 3.
Received event: 4.
Received event: 5.
Received event: 6.
Received event: 7.
Received event: 8.
Received event: 9.
Received event: 10.
```

### Actual Output:
```
Subscribe Complete
Emitting events...
Received event: 6.
Received event: 7.
Received event: 8.
Received event: 9.
Received event: 10.
```

## Solution
It seems like it would be possible for the PersistentStreamPullingAgent to create an initial cursor for the Consumer before completeing the asynchronous AddSubscriber call. This would ensure that any events that occur during the handshake cant get evicted from the cache until it completes. Then if the Consumer had actually specified an initial stream token, it can simply update the cursor, but if it didn't then it'll start processing those events as expected from the Consumer's point of view.