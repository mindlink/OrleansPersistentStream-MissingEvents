# Overview
When the PersistentStreamPullingAgent already has an active stream and a new explicit subscription is setup by a Consumer, if the handshake takes a bit of time then events can appear to go missing from the Consumer's point of view after the SubscribeAsync finishes on the StreamProvider.

## The Problem
We expect Orleans to honor the contract described here https://learn.microsoft.com/en-us/dotnet/orleans/implementation/streams-implementation/:

>This handshake between the agent and the pub-sub guarantees strong streaming subscription semantics: **once the consumer has subscribed to the stream it will see all events that were generated after it has subscribed**."

When the Consumer subscribes, the call goes via a PubSubGrain to the PersistentStreamPullingAgent. It kicks off some asynchronous work to perform the Consumer handshake and get the initial stream token if the Consumer specified one. The Task is rightly not awaited here (because of potential deadlocks) but it means that the Consumer's call to the SubscribeAsync on the StreamProvider completes before the PersistentStreamPullingAgent has actually created a cursor for the Consumer.

If the handshake takes a bit of time because it's waiting for the Consumer grain to do more work after successfully subscribing and any new events occur on the stream at the same time, the agent can process them all and evict them from the cache before the handshake completes, so the Consumer never gets them.

From the Consumer's point of view it looks like the events are missing.

This is problematic in our case because our real Consumer grains fetch the versioned state of the model from the DB after they've subscribed to their event stream to get up to date. The Producer models update their state in the DB and then emit events to notify Consumers. We rely on the fact that we can coordinate subscribing _and then_ fetching the state to ensure that no events can be missed in the interim.

We've used MemoryStreams here to show the issue, but in our actual code we've written a custom stream provider which uses a SimpleQueueCache. It's not rewindable so we don't use Consumer stream tokens on Subscribe at all.

## Suggested fix
Instead it seems we should interpret the contract as: 

> ...once the consumer has subscribed to the stream it will see all events that were generated after it has subscribed, **from the next turn**."

By making the producer yield after subscription and then retrieving state on the next turn, we are guaranteed to be subscribed by the time the state retrieval occurs.

## Repro
This project uses two grains to re-create the race condition described above:

1. `ProducerGrain` - incrementally mutates its "state" by storing the new state in a persistent `StateStore` and then publishing an event representing the state change.
2. `ConsumerGrain` - trie to monitor changes to the producers state by subscribing to the producers mutation event stream, fetching the current state from the event store, and then applying future mutation events on top of the state.

We use two helper components:

1. `StateStore` - Simulates a persistent store that has an async I/O delay when the consumer performs the initial fetch.
2. `TestCompletionExaminationService` -- Records state and events seen by the consumer and computes the correct "effective" state, given the retrieved state and the observed events, declaring the test as completed when the consumer has observed all expected mutations.

### Expected Output (running in "fixed" mode - i.e. making the consumer yield after subscription and before state retreival):
```
CONSUMER: Handling completion of subscription by enqueuing producer mutation.
CONSUMER: Enqueuing getting state on next turn as mode is fixed.
CONSUMER: Invoking producer mutation for 20 mutation events.
CONSUMER: Beginning getting state.
PRODUCER: Mutating state from <none>
PRODUCER: Setting state as 0
PRODUCER: Publishing mutation event as 0
PRODUCER: Awaiting after publication of mutation event as 0
CONSUMER: Handling receival of event: 0 by reporting
PRODUCER: Setting state as 1
PRODUCER: Publishing mutation event as 1
PRODUCER: Awaiting after publication of mutation event as 1
PRODUCER: Setting state as 2
PRODUCER: Publishing mutation event as 2
PRODUCER: Awaiting after publication of mutation event as 2
CONSUMER: Handling receival of event: 1 by reporting
CONSUMER: Handling receival of event: 2 by reporting
PRODUCER: Setting state as 3
PRODUCER: Publishing mutation event as 3
PRODUCER: Awaiting after publication of mutation event as 3
PRODUCER: Setting state as 4
PRODUCER: Publishing mutation event as 4
PRODUCER: Awaiting after publication of mutation event as 4
CONSUMER: Handling receival of event: 3 by reporting
CONSUMER: Handling receival of event: 4 by reporting
PRODUCER: Setting state as 5
PRODUCER: Publishing mutation event as 5
PRODUCER: Awaiting after publication of mutation event as 5
PRODUCER: Setting state as 6
CONSUMER: Handling receival of event: 5 by reporting
PRODUCER: Publishing mutation event as 6
PRODUCER: Awaiting after publication of mutation event as 6
PRODUCER: Setting state as 7
PRODUCER: Publishing mutation event as 7
PRODUCER: Awaiting after publication of mutation event as 7
PRODUCER: Setting state as 8
PRODUCER: Publishing mutation event as 8
CONSUMER: Handling receival of event: 6 by reporting
PRODUCER: Awaiting after publication of mutation event as 8
CONSUMER: Handling receival of event: 7 by reporting
CONSUMER: Handling receival of event: 8 by reporting
PRODUCER: Setting state as 9
PRODUCER: Publishing mutation event as 9
PRODUCER: Awaiting after publication of mutation event as 9
PRODUCER: Setting state as 10
PRODUCER: Publishing mutation event as 10
CONSUMER: Handling receival of event: 9 by reporting
PRODUCER: Awaiting after publication of mutation event as 10
CONSUMER: Handling receival of event: 10 by reporting
PRODUCER: Setting state as 11
PRODUCER: Publishing mutation event as 11
PRODUCER: Awaiting after publication of mutation event as 11
PRODUCER: Setting state as 12
PRODUCER: Publishing mutation event as 12
CONSUMER: Handling receival of event: 11 by reporting
PRODUCER: Awaiting after publication of mutation event as 12
PRODUCER: Setting state as 13
PRODUCER: Publishing mutation event as 13
PRODUCER: Awaiting after publication of mutation event as 13
PRODUCER: Setting state as 14
CONSUMER: Handling receival of event: 12 by reporting
PRODUCER: Publishing mutation event as 14
CONSUMER: Handling receival of event: 13 by reporting
PRODUCER: Awaiting after publication of mutation event as 14
PRODUCER: Setting state as 15
PRODUCER: Publishing mutation event as 15
PRODUCER: Awaiting after publication of mutation event as 15
CONSUMER: Handling receival of event: 14 by reporting
PRODUCER: Setting state as 16
PRODUCER: Publishing mutation event as 16
PRODUCER: Awaiting after publication of mutation event as 16
CONSUMER: Handling receival of event: 15 by reporting
PRODUCER: Setting state as 17
PRODUCER: Publishing mutation event as 17
PRODUCER: Awaiting after publication of mutation event as 17
CONSUMER: Handling receival of event: 16 by reporting
CONSUMER: Handling receival of event: 17 by reporting
PRODUCER: Setting state as 18
CONSUMER: Reporting initial state as: 8
PRODUCER: Publishing mutation event as 18
PRODUCER: Awaiting after publication of mutation event as 18
PRODUCER: Setting state as 19
PRODUCER: Publishing mutation event as 19
PRODUCER: Awaiting after publication of mutation event as 19
CONSUMER: Handling receival of event: 18 by reporting
CONSUMER: Handling receival of event: 19 by reporting
PASS: Test passed as 'Correct effective state '19' was resolved.' with calculated effective state 19 from retrieved
state 8 and observed events 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19.
```

### Actual Output (running in "broken" mode - i.e. blocking the consumer grain scheduler by retrieving state immediately after subscribing
```
CONSUMER: Handling completion of subscription by enqueuing producer mutation.
CONSUMER: Getting state immediately as mode is broken.
CONSUMER: Invoking producer mutation for 20 mutation events.
PRODUCER: Mutating state from <none>
PRODUCER: Setting state as 0
PRODUCER: Publishing mutation event as 0
PRODUCER: Awaiting after publication of mutation event as 0
PRODUCER: Setting state as 1
PRODUCER: Publishing mutation event as 1
PRODUCER: Awaiting after publication of mutation event as 1
PRODUCER: Setting state as 2
PRODUCER: Publishing mutation event as 2
PRODUCER: Awaiting after publication of mutation event as 2
PRODUCER: Setting state as 3
PRODUCER: Publishing mutation event as 3
PRODUCER: Awaiting after publication of mutation event as 3
PRODUCER: Setting state as 4
PRODUCER: Publishing mutation event as 4
PRODUCER: Awaiting after publication of mutation event as 4
PRODUCER: Setting state as 5
PRODUCER: Publishing mutation event as 5
PRODUCER: Awaiting after publication of mutation event as 5
PRODUCER: Setting state as 6
PRODUCER: Publishing mutation event as 6
PRODUCER: Awaiting after publication of mutation event as 6
PRODUCER: Setting state as 7
PRODUCER: Publishing mutation event as 7
PRODUCER: Awaiting after publication of mutation event as 7
PRODUCER: Setting state as 8
PRODUCER: Publishing mutation event as 8
PRODUCER: Awaiting after publication of mutation event as 8
PRODUCER: Setting state as 9
PRODUCER: Publishing mutation event as 9
PRODUCER: Awaiting after publication of mutation event as 9
PRODUCER: Setting state as 10
PRODUCER: Publishing mutation event as 10
PRODUCER: Awaiting after publication of mutation event as 10
PRODUCER: Setting state as 11
PRODUCER: Publishing mutation event as 11
PRODUCER: Awaiting after publication of mutation event as 11
PRODUCER: Setting state as 12
PRODUCER: Publishing mutation event as 12
PRODUCER: Awaiting after publication of mutation event as 12
PRODUCER: Setting state as 13
PRODUCER: Publishing mutation event as 13
PRODUCER: Awaiting after publication of mutation event as 13
PRODUCER: Setting state as 14
PRODUCER: Publishing mutation event as 14
PRODUCER: Awaiting after publication of mutation event as 14
PRODUCER: Setting state as 15
PRODUCER: Publishing mutation event as 15
PRODUCER: Awaiting after publication of mutation event as 15
PRODUCER: Setting state as 16
PRODUCER: Publishing mutation event as 16
PRODUCER: Awaiting after publication of mutation event as 16
PRODUCER: Setting state as 17
PRODUCER: Publishing mutation event as 17
PRODUCER: Awaiting after publication of mutation event as 17
PRODUCER: Setting state as 18
PRODUCER: Publishing mutation event as 18
PRODUCER: Awaiting after publication of mutation event as 18
CONSUMER: Reporting initial retrieved state as: 9
PRODUCER: Setting state as 19
PRODUCER: Publishing mutation event as 19
PRODUCER: Awaiting after publication of mutation event as 19
CONSUMER: Handling receival of event: 18 by reporting
CONSUMER: Handling receival of event: 19 by reporting
FAIL: Test failed as 'A final state mutation event was received for the expected effective state '19' but the actual
effective state could not be calculated on top of the existing state.' with calculated effective state 9, retrieved
state 9, and observed events 18, 19.
```
