namespace OrleansMissingEvents
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;

    internal class TestCompletionExaminationService
    {
        private readonly int expectedCompletionState;

        private readonly ConcurrentDictionary<Guid, TestCompletionExaminer> testCompletionExaminersByTestId =
            new ConcurrentDictionary<Guid, TestCompletionExaminer>();

        public TestCompletionExaminationService(int expectedCompletionState)
        {
            if (expectedCompletionState <= 0)
            {
                throw new ArgumentException("Expected completion state must be greater than zero", nameof(expectedCompletionState));
            }

            this.expectedCompletionState = expectedCompletionState;
        }

        public void ReportStateRetrieved(Guid testId, int? state)
        {
            var testCompletionExaminer = this.GetOrCreateTestCompletionExaminer(testId);

            testCompletionExaminer.ReportStateRetrieved(state);
        }

        public void ReportObservedMutationEvent(Guid testId, int mutationEvent)
        {
            var testCompletionExaminer = this.GetOrCreateTestCompletionExaminer(testId);

            testCompletionExaminer.ReportObservedMutationEvent(mutationEvent);
        }

        public Task<TestResults> AwaitTestCompletion(Guid testId)
        {
            var testCompletionExaminer = this.GetOrCreateTestCompletionExaminer(testId);

            return testCompletionExaminer.AwaitTestCompletion();
        }

        private TestCompletionExaminer GetOrCreateTestCompletionExaminer(Guid testId)
        {
            return this.testCompletionExaminersByTestId.GetOrAdd(testId,
                _ => new TestCompletionExaminer(this.expectedCompletionState));
        }
    }
}
