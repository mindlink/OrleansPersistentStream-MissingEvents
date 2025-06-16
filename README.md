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
Intercept the grain call to perform the handshake between the PersistentStreamPullingAgent and the consumer. As the call completes, enqueue a job on the PersistentStreamPullingAgent scheduling queue to notify the consumer that the handshake has completed.

This works because:
1) The scheduled job will run after the current PersistentStreamPullingAgent turn has completed (i.e. after the handshake grain call has returned and been processed)
2) The notification to the consumer grain will be run on the consumer's turn queue, and can be used to perform any necessary continuation logic.

## Repro
This project uses two grains to re-create the race condition described above:

1. `ProducerGrain` - incrementally mutates its "state" by storing the new state in a persistent `StateStore` and then publishing an event representing the state change.
2. `ConsumerGrain` - trie to monitor changes to the producers state by subscribing to the producers mutation event stream, fetching the current state from the event store, and then applying future mutation events on top of the state.

We use a grain filter to simulate delayed calls between PersistentStreamPullingAgent and consumers:

1. `DelayedSubscriptionOutgoingGrainCallFilter` - interleaves a delay if the call is a subscription call between PersistentStreamPullingAgent and a consumer.

We use two helper components:

1. `StateStore` - Simulates a persistent store that has an async I/O delay when the consumer performs the initial fetch.
2. `TestCompletionExaminationService` -- Records state and events seen by the consumer and computes the correct "effective" state, given the retrieved state and the observed events, declaring the test as completed when the consumer has observed all expected mutations.

We also offer an implementation for the fix, including:

1. `ReliableSubscriptionGrainExtension` - A grain extension to expose the additional step in the subscription handshake.
1. `ReliableSubscriptionManager` - A stateful class to coordinate subscription and continuation.
1. `ReliableSubscriptionOutgoingGrainCallFilter` - A grain filter to intercept the vanilla handshake and queue an additional call to the consumer grain.

### Expected Output (running in "fixed" mode - i.e. making the consumer yield after subscription and before state retreival):
```
CONSUMER: Invoking producer mutation for 40 mutation events.
CONSUMER: Reliably subscribing as the test mode is fixed.
PRODUCER: Mutating state from <none>.
PRODUCER: Setting state as 0.
PRODUCER: Publishing mutation event as 0.
PRODUCER: Awaiting after publication of mutation event as 0.
CONSUMER: Handling completion of reliable subscription by taking no action.
PRODUCER: Setting state as 1.
PRODUCER: Publishing mutation event as 1.
PRODUCER: Awaiting after publication of mutation event as 1.
PRODUCER: Setting state as 2.
PRODUCER: Publishing mutation event as 2.
PRODUCER: Awaiting after publication of mutation event as 2.
PRODUCER: Setting state as 3.
PRODUCER: Publishing mutation event as 3.
PRODUCER: Awaiting after publication of mutation event as 3.
PRODUCER: Setting state as 4.
PRODUCER: Publishing mutation event as 4.
PRODUCER: Awaiting after publication of mutation event as 4.
PRODUCER: Setting state as 5.
PRODUCER: Publishing mutation event as 5.
PRODUCER: Awaiting after publication of mutation event as 5.
PRODUCER: Setting state as 6.
PRODUCER: Publishing mutation event as 6.
PRODUCER: Awaiting after publication of mutation event as 6.
PRODUCER: Setting state as 7.
PRODUCER: Publishing mutation event as 7.
PRODUCER: Awaiting after publication of mutation event as 7.
PRODUCER: Setting state as 8.
PRODUCER: Publishing mutation event as 8.
PRODUCER: Awaiting after publication of mutation event as 8.
PRODUCER: Setting state as 9.
PRODUCER: Publishing mutation event as 9.
PRODUCER: Awaiting after publication of mutation event as 9.
PRODUCER: Setting state as 10.
PRODUCER: Publishing mutation event as 10.
PRODUCER: Awaiting after publication of mutation event as 10.
PRODUCER: Setting state as 11.
PRODUCER: Publishing mutation event as 11.
PRODUCER: Awaiting after publication of mutation event as 11.
PRODUCER: Setting state as 12.
PRODUCER: Publishing mutation event as 12.
PRODUCER: Awaiting after publication of mutation event as 12.
PRODUCER: Setting state as 13.
PRODUCER: Publishing mutation event as 13.
PRODUCER: Awaiting after publication of mutation event as 13.
PRODUCER: Setting state as 14.
PRODUCER: Publishing mutation event as 14.
PRODUCER: Awaiting after publication of mutation event as 14.
PRODUCER: Setting state as 15.
PRODUCER: Publishing mutation event as 15.
PRODUCER: Awaiting after publication of mutation event as 15.
PRODUCER: Setting state as 16.
PRODUCER: Publishing mutation event as 16.
PRODUCER: Awaiting after publication of mutation event as 16.
PRODUCER: Setting state as 17.
PRODUCER: Publishing mutation event as 17.
PRODUCER: Awaiting after publication of mutation event as 17.
CONSUMER: Beginning continuation of reliable subscription by getting initial state on new turn.
PRODUCER: Setting state as 18.
PRODUCER: Publishing mutation event as 18.
PRODUCER: Awaiting after publication of mutation event as 18.
PRODUCER: Setting state as 19.
PRODUCER: Publishing mutation event as 19.
PRODUCER: Awaiting after publication of mutation event as 19.
PRODUCER: Setting state as 20.
PRODUCER: Publishing mutation event as 20.
PRODUCER: Awaiting after publication of mutation event as 20.
PRODUCER: Setting state as 21.
PRODUCER: Publishing mutation event as 21.
PRODUCER: Awaiting after publication of mutation event as 21.
PRODUCER: Setting state as 22.
PRODUCER: Publishing mutation event as 22.
PRODUCER: Awaiting after publication of mutation event as 22.
PRODUCER: Setting state as 23.
PRODUCER: Publishing mutation event as 23.
PRODUCER: Awaiting after publication of mutation event as 23.
PRODUCER: Setting state as 24.
PRODUCER: Publishing mutation event as 24.
PRODUCER: Awaiting after publication of mutation event as 24.
PRODUCER: Setting state as 25.
PRODUCER: Publishing mutation event as 25.
PRODUCER: Awaiting after publication of mutation event as 25.
PRODUCER: Setting state as 26.
PRODUCER: Publishing mutation event as 26.
PRODUCER: Awaiting after publication of mutation event as 26.
PRODUCER: Setting state as 27.
PRODUCER: Publishing mutation event as 27.
PRODUCER: Awaiting after publication of mutation event as 27.
PRODUCER: Setting state as 28.
PRODUCER: Publishing mutation event as 28.
PRODUCER: Awaiting after publication of mutation event as 28.
PRODUCER: Setting state as 29.
PRODUCER: Publishing mutation event as 29.
PRODUCER: Awaiting after publication of mutation event as 29.
PRODUCER: Setting state as 30.
PRODUCER: Publishing mutation event as 30.
PRODUCER: Awaiting after publication of mutation event as 30.
PRODUCER: Setting state as 31.
PRODUCER: Publishing mutation event as 31.
PRODUCER: Awaiting after publication of mutation event as 31.
PRODUCER: Setting state as 32.
PRODUCER: Publishing mutation event as 32.
PRODUCER: Awaiting after publication of mutation event as 32.
PRODUCER: Setting state as 33.
PRODUCER: Publishing mutation event as 33.
PRODUCER: Awaiting after publication of mutation event as 33.
CONSUMER: Handling completion of initial retrieved state as '25' by reporting to test examiner.
CONSUMER: Handling receival of even '16' by reporting to test examiner.
CONSUMER: Handling receival of even '17' by reporting to test examiner.
CONSUMER: Handling receival of even '18' by reporting to test examiner.
CONSUMER: Handling receival of even '19' by reporting to test examiner.
CONSUMER: Handling receival of even '20' by reporting to test examiner.
CONSUMER: Handling receival of even '21' by reporting to test examiner.
CONSUMER: Handling receival of even '22' by reporting to test examiner.
CONSUMER: Handling receival of even '23' by reporting to test examiner.
CONSUMER: Handling receival of even '24' by reporting to test examiner.
CONSUMER: Handling receival of even '25' by reporting to test examiner.
CONSUMER: Handling receival of even '26' by reporting to test examiner.
CONSUMER: Handling receival of even '27' by reporting to test examiner.
CONSUMER: Handling receival of even '28' by reporting to test examiner.
CONSUMER: Handling receival of even '29' by reporting to test examiner.
CONSUMER: Handling receival of even '30' by reporting to test examiner.
CONSUMER: Handling receival of even '31' by reporting to test examiner.
CONSUMER: Handling receival of even '32' by reporting to test examiner.
CONSUMER: Handling receival of even '33' by reporting to test examiner.
PRODUCER: Setting state as 34.
PRODUCER: Publishing mutation event as 34.
PRODUCER: Awaiting after publication of mutation event as 34.
PRODUCER: Setting state as 35.
PRODUCER: Publishing mutation event as 35.
PRODUCER: Awaiting after publication of mutation event as 35.
CONSUMER: Handling receival of even '34' by reporting to test examiner.
CONSUMER: Handling receival of even '35' by reporting to test examiner.
PRODUCER: Setting state as 36.
PRODUCER: Publishing mutation event as 36.
PRODUCER: Awaiting after publication of mutation event as 36.
PRODUCER: Setting state as 37.
PRODUCER: Publishing mutation event as 37.
PRODUCER: Awaiting after publication of mutation event as 37.
CONSUMER: Handling receival of even '36' by reporting to test examiner.
CONSUMER: Handling receival of even '37' by reporting to test examiner.
PRODUCER: Setting state as 38.
PRODUCER: Publishing mutation event as 38.
PRODUCER: Awaiting after publication of mutation event as 38.
CONSUMER: Handling receival of even '38' by reporting to test examiner.
PRODUCER: Setting state as 39.
PRODUCER: Publishing mutation event as 39.
PRODUCER: Awaiting after publication of mutation event as 39.
CONSUMER: Handling receival of even '39' by reporting to test examiner.
PASS: Test passed as 'Correct effective state '39' was resolved.' with calculated effective state 39 from retrieved
state 25 and observed events 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
39.
```

### Actual Output (running in "broken" mode - i.e. blocking the consumer grain scheduler by retrieving state immediately after subscribing
```
CONSUMER: Invoking producer mutation for 40 mutation events.
CONSUMER: Non-reliable subscribing as the test mode is broken.
PRODUCER: Mutating state from <none>.
PRODUCER: Setting state as 0.
PRODUCER: Publishing mutation event as 0.
PRODUCER: Awaiting after publication of mutation event as 0.
CONSUMER: Handling completion of non-reliable subscription by getting initial state immediately.
PRODUCER: Setting state as 1.
PRODUCER: Publishing mutation event as 1.
PRODUCER: Awaiting after publication of mutation event as 1.
PRODUCER: Setting state as 2.
PRODUCER: Publishing mutation event as 2.
PRODUCER: Awaiting after publication of mutation event as 2.
PRODUCER: Setting state as 3.
PRODUCER: Publishing mutation event as 3.
PRODUCER: Awaiting after publication of mutation event as 3.
PRODUCER: Setting state as 4.
PRODUCER: Publishing mutation event as 4.
PRODUCER: Awaiting after publication of mutation event as 4.
PRODUCER: Setting state as 5.
PRODUCER: Publishing mutation event as 5.
PRODUCER: Awaiting after publication of mutation event as 5.
PRODUCER: Setting state as 6.
PRODUCER: Publishing mutation event as 6.
PRODUCER: Awaiting after publication of mutation event as 6.
PRODUCER: Setting state as 7.
PRODUCER: Publishing mutation event as 7.
PRODUCER: Awaiting after publication of mutation event as 7.
PRODUCER: Setting state as 8.
PRODUCER: Publishing mutation event as 8.
PRODUCER: Awaiting after publication of mutation event as 8.
PRODUCER: Setting state as 9.
PRODUCER: Publishing mutation event as 9.
PRODUCER: Awaiting after publication of mutation event as 9.
PRODUCER: Setting state as 10.
PRODUCER: Publishing mutation event as 10.
PRODUCER: Awaiting after publication of mutation event as 10.
PRODUCER: Setting state as 11.
PRODUCER: Publishing mutation event as 11.
PRODUCER: Awaiting after publication of mutation event as 11.
PRODUCER: Setting state as 12.
PRODUCER: Publishing mutation event as 12.
PRODUCER: Awaiting after publication of mutation event as 12.
PRODUCER: Setting state as 13.
PRODUCER: Publishing mutation event as 13.
PRODUCER: Awaiting after publication of mutation event as 13.
PRODUCER: Setting state as 14.
PRODUCER: Publishing mutation event as 14.
PRODUCER: Awaiting after publication of mutation event as 14.
PRODUCER: Setting state as 15.
PRODUCER: Publishing mutation event as 15.
PRODUCER: Awaiting after publication of mutation event as 15.
PRODUCER: Setting state as 16.
PRODUCER: Publishing mutation event as 16.
PRODUCER: Awaiting after publication of mutation event as 16.
CONSUMER: Handling completion of initial retrieved state as '8' by reporting to test examiner.
PRODUCER: Setting state as 17.
PRODUCER: Publishing mutation event as 17.
PRODUCER: Awaiting after publication of mutation event as 17.
CONSUMER: Handling receival of even '16' by reporting to test examiner.
CONSUMER: Handling receival of even '17' by reporting to test examiner.
PRODUCER: Setting state as 18.
PRODUCER: Publishing mutation event as 18.
PRODUCER: Awaiting after publication of mutation event as 18.
PRODUCER: Setting state as 19.
PRODUCER: Publishing mutation event as 19.
PRODUCER: Awaiting after publication of mutation event as 19.
CONSUMER: Handling receival of even '18' by reporting to test examiner.
CONSUMER: Handling receival of even '19' by reporting to test examiner.
PRODUCER: Setting state as 20.
PRODUCER: Publishing mutation event as 20.
PRODUCER: Awaiting after publication of mutation event as 20.
PRODUCER: Setting state as 21.
PRODUCER: Publishing mutation event as 21.
PRODUCER: Awaiting after publication of mutation event as 21.
CONSUMER: Handling receival of even '20' by reporting to test examiner.
CONSUMER: Handling receival of even '21' by reporting to test examiner.
PRODUCER: Setting state as 22.
PRODUCER: Publishing mutation event as 22.
PRODUCER: Awaiting after publication of mutation event as 22.
CONSUMER: Handling receival of even '22' by reporting to test examiner.
PRODUCER: Setting state as 23.
PRODUCER: Publishing mutation event as 23.
PRODUCER: Awaiting after publication of mutation event as 23.
PRODUCER: Setting state as 24.
PRODUCER: Publishing mutation event as 24.
PRODUCER: Awaiting after publication of mutation event as 24.
CONSUMER: Handling receival of even '23' by reporting to test examiner.
CONSUMER: Handling receival of even '24' by reporting to test examiner.
PRODUCER: Setting state as 25.
PRODUCER: Publishing mutation event as 25.
PRODUCER: Awaiting after publication of mutation event as 25.
PRODUCER: Setting state as 26.
PRODUCER: Publishing mutation event as 26.
PRODUCER: Awaiting after publication of mutation event as 26.
CONSUMER: Handling receival of even '25' by reporting to test examiner.
CONSUMER: Handling receival of even '26' by reporting to test examiner.
PRODUCER: Setting state as 27.
PRODUCER: Publishing mutation event as 27.
PRODUCER: Awaiting after publication of mutation event as 27.
PRODUCER: Setting state as 28.
PRODUCER: Publishing mutation event as 28.
PRODUCER: Awaiting after publication of mutation event as 28.
CONSUMER: Handling receival of even '27' by reporting to test examiner.
CONSUMER: Handling receival of even '28' by reporting to test examiner.
PRODUCER: Setting state as 29.
PRODUCER: Publishing mutation event as 29.
PRODUCER: Awaiting after publication of mutation event as 29.
PRODUCER: Setting state as 30.
PRODUCER: Publishing mutation event as 30.
PRODUCER: Awaiting after publication of mutation event as 30.
CONSUMER: Handling receival of even '29' by reporting to test examiner.
CONSUMER: Handling receival of even '30' by reporting to test examiner.
PRODUCER: Setting state as 31.
PRODUCER: Publishing mutation event as 31.
PRODUCER: Awaiting after publication of mutation event as 31.
CONSUMER: Handling receival of even '31' by reporting to test examiner.
PRODUCER: Setting state as 32.
PRODUCER: Publishing mutation event as 32.
PRODUCER: Awaiting after publication of mutation event as 32.
PRODUCER: Setting state as 33.
PRODUCER: Publishing mutation event as 33.
PRODUCER: Awaiting after publication of mutation event as 33.
CONSUMER: Handling receival of even '32' by reporting to test examiner.
CONSUMER: Handling receival of even '33' by reporting to test examiner.
PRODUCER: Setting state as 34.
PRODUCER: Publishing mutation event as 34.
PRODUCER: Awaiting after publication of mutation event as 34.
PRODUCER: Setting state as 35.
PRODUCER: Publishing mutation event as 35.
PRODUCER: Awaiting after publication of mutation event as 35.
CONSUMER: Handling receival of even '34' by reporting to test examiner.
CONSUMER: Handling receival of even '35' by reporting to test examiner.
PRODUCER: Setting state as 36.
PRODUCER: Publishing mutation event as 36.
PRODUCER: Awaiting after publication of mutation event as 36.
PRODUCER: Setting state as 37.
PRODUCER: Publishing mutation event as 37.
PRODUCER: Awaiting after publication of mutation event as 37.
CONSUMER: Handling receival of even '36' by reporting to test examiner.
CONSUMER: Handling receival of even '37' by reporting to test examiner.
PRODUCER: Setting state as 38.
PRODUCER: Publishing mutation event as 38.
PRODUCER: Awaiting after publication of mutation event as 38.
PRODUCER: Setting state as 39.
PRODUCER: Publishing mutation event as 39.
PRODUCER: Awaiting after publication of mutation event as 39.
CONSUMER: Handling receival of even '38' by reporting to test examiner.
CONSUMER: Handling receival of even '39' by reporting to test examiner.
FAIL: Test failed as 'A final state mutation event was received for the expected effective state '39' but the actual
effective state could not be calculated on top of the existing state.' with calculated effective state 8, retrieved
state 8, and observed events 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
39.
```
