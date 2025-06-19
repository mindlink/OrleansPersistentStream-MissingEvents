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

## Repro
This project uses two grains to re-create the race condition described above:

1. `ProducerGrain` - incrementally mutates its "state" by storing the new state in a persistent `StateStore` and then publishing an event representing the state change.
2. `ConsumerGrain` - tries to monitor changes to the producers state by subscribing to the producers mutation event stream, fetching the current state from the event store, and then applying future mutation events on top of the state.

We use a grain filter to simulate delayed calls between PersistentStreamPullingAgent and consumers:

1. `DelayedSubscriptionOutgoingGrainCallFilter` - interleaves a delay if the call is a subscription call between PersistentStreamPullingAgent and a consumer.

We use two helper components:

1. `StateStore` - Simulates a persistent store that has an async I/O delay when the consumer performs the initial fetch.
2. `TestCompletionExaminationService` - Records state and events seen by the consumer and computes the correct "effective" state, given the retrieved state and the observed events, declaring the test as completed when the consumer has observed all expected mutations.

## Suggested fix
1. Intercept the grain persistent pulling agent stream subscription, the underlying consumer grain handshake, and the queue cache interactions via grain filters and stream provider decorator injection, respectively.
2. Observe the stream subscription process via interception and pre-emptively record the stream subscription as pending until it either completes the subscribing process in its entirety, the initial subscribing interaction fails, or the underlying consumer handshake fails.
3. As events are added to the cache, if there's a current pending stream subscription in progress, create a "pinning" cursor in the cache to ensure the concurrently-received events are not evicted.
4. When a stream susbcription completes its subscribing process, if a pinning cursor has indeed been created during its lifetime, start the stream subscription from there to replay the concurrently-received events.
5. Ensure all failure cases clean up any pinning tokens and remove any in-flight data.

### Solution Pros
1. No additional grain calls or other I/O latency.
2. Zero changes required to application code, just drop-in replace the streaming provider.
3. Uses the native semantics of the caching and stream sequence token mechanisms.
4. Does not interfere with grain threading or scheduling models.
5. Zero overhead in sunny-day scenarios (i.e. when there is no concurrent subscription/event publication).

### Solution Cons
1. Requires reflection of the non-public QueueId property from the PersistentStreamPullingAgent.
2. Makes non-contractual implicit assumptions about the scheduling and call-stack model of the stream provider implementation.
> *How likely are these to change, and what options do we have if they do?*

## Fix Implemenation
We intercept the streaming mechanism at two "sandwich layers" to ensure that events are not purged whilst pending subscriptions are in-flight.

The an in-flight subscription lifecycle is considered to be:
1. Started on request to subscribe to the PersistentStreamPullingAgent.
2. Completed when an initial cursor is requested for the subscription from the queue cache.
3. Failed if the request to the PersistentStreamPullingAgent fails.
4. Failed if the underlying handshake back to the consumer fails.

At the core of the solution, we use a shared mechanism to track pending stream subscriptions:

1. `PendingStreamSubscriptionsManager` - Tracks pending stream subscriptions for a stream on a given queue. Designed to be hosted in-memory alongside each `PersistentStreamPullingAgent`.
2. `PendingStreamSubscriptionsCoordinationService` - A service that allows the per-queue stream subscription managers to be retrieved globally for the local silo.

On one side of the interception sandwich, we intercept the 'consumer subscription' layer to track subscription lifecycle and activity:

1. `StreamSubscriptionIncomingGrainCallFilter` - Intercepts inbound requests into the `PersistentStreamPullingAgent`, registers the stream subscriber as pending, and cleans up on failure to subscribe. This mechanism also sets the invoked parameters on the Orleans request context so downstream interception components are able to resolve them.
2. `ConsumerHandshakeOutgoingGrainCallFilter` - Intercepts the consumer handshake and cleans up any residual pending stream subscription data if this process fails, using parameters flowed from the original stream request.

On the other side of the interception sandwich, we intercept the Persistent Pulling Stream Agent's 'state' layer (the queue cache) to:

1. Ensure any newly received events are cached if there are current pending subscriptions are in flight, by creating artificial pinning cursors for each pending subscription.
2. Identify when subscription handshakes complete successfully (i.e. when a queue cache cursor is being requested), and clean up any pinning cursors that may have been created if concurrent events have been received.

We wrap an underlying stream provider queue caching mechanism and inject our interception logic:

1. `ReliableStreamsQueueCache` - Wraps an underlying queue cache, notifying when events are received (in case pending subscriptions are in flight), and treating cursor requests as confirmation of stream subscription completion using parameters flowed from the initial stream subscription invocation.
2. `ReliableStreamsQueueAdaptorCache` - Plugs in to the stream provider mechanism and creates the intercepting cache per-queue.
3. `ReliableStreamsQueueAdaptorFactory` - Plugs in to the stream provider mechanism to create the overall intercepting components.

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
