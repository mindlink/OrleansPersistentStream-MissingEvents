namespace OrleansMissingEvents.TestHarness;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

internal class TestCompletionExaminer
{
    private readonly int expectedCompletionState;

    private TaskCompletionSource<TestResults> testCompletionTaskCompletionSource = new TaskCompletionSource<TestResults>();

    private int? retrievedState;

    private readonly List<int> observedMutationEvents = [];

    public TestCompletionExaminer(int expectedCompletionState)
    {
        if (expectedCompletionState <= 0)
        {
            throw new ArgumentException("Expected completion state must be greater than zero", nameof(expectedCompletionState));
        }

        this.expectedCompletionState = expectedCompletionState;
    }

    public void ReportStateRetrieved(int? state)
    {
        if (state < 0)
        {
            throw new ArgumentException("State value must be greater than or equal to zero if provided", nameof(state));
        }

        if (state > expectedCompletionState)
        {
            throw new ArgumentException($"State value must be less than or equal to the expected completion state '{expectedCompletionState}' if provided.", nameof(state));
        }

        if (retrievedState != null)
        {
            throw new InvalidOperationException($"State has already been reported as '{retrievedState}'.");
        }

        retrievedState = state ?? -1;

        TryDeclareTestCompleted();
    }

    public void ReportObservedMutationEvent(int mutationEvent)
    {
        if (mutationEvent < 0)
        {
            throw new ArgumentException("Mutation event must be greater than or equal to zero", nameof(mutationEvent));
        }

        observedMutationEvents.Add(mutationEvent);

        TryDeclareTestCompleted();
    }

    public Task<TestResults> AwaitTestCompletion()
    {
        return testCompletionTaskCompletionSource.Task;
    }

    private void TryDeclareTestCompleted()
    {
        // We should have had all events being received in order.
        if (observedMutationEvents.Select((observedMutationEvent, index) =>
                index != 0 && observedMutationEvents[index - 1] != observedMutationEvent - 1).Any(v => v))
        {
            testCompletionTaskCompletionSource.TrySetResult(
                CreateTestResults(TestStatus.Fail, "Contiguous mutation events were not observed.", retrievedState));

            return;
        }

        // If we haven't retrieved state yet then we can't complete.
        if (retrievedState == null)
        {
            return;
        }

        // Calculate the effective state by applying all observed events incrementally in order since the retrieved state.
        var effectiveState = observedMutationEvents.Aggregate(
            retrievedState,
            (currentEffectiveState, observedMutationEvent) => observedMutationEvent == currentEffectiveState + 1
                ? observedMutationEvent : currentEffectiveState);

        // We're done if we're at the expected state
        if (effectiveState == expectedCompletionState)
        {
            testCompletionTaskCompletionSource.TrySetResult(
                CreateTestResults(TestStatus.Pass, $"Correct effective state '{expectedCompletionState}' was resolved.", effectiveState));

            return;
        }

        // Something has gone wrong if we've received all events and gotten the state, but the effective state calculation hasn't worked (above).
        if (retrievedState != null &&
            observedMutationEvents.Any() && observedMutationEvents.Last() == expectedCompletionState)
        {
            testCompletionTaskCompletionSource.TrySetResult(
                CreateTestResults(
                    TestStatus.Fail,
                    $"A final state mutation event was received for the expected effective state '{expectedCompletionState}' but the actual effective state could not be calculated on top of the existing state.",
                    retrievedState));
        }
    }

    private TestResults CreateTestResults(TestStatus testStatus, string description, int? effectiveState)
    {
        return new TestResults(testStatus, description, effectiveState, retrievedState, observedMutationEvents);
    }
}