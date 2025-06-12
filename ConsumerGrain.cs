namespace OrleansMissingEvents
{
    using Orleans.Runtime;
    using Orleans.Streams;

    internal class ConsumerGrain(StateStore stateStore, TestCompletionExaminationService testCompletionExaminationService, CommandLineInterface commandLineInterface) : Grain, IConsumerGrain
    {
        private readonly StateStore stateStore = stateStore;

        private readonly TestCompletionExaminationService testCompletionExaminationService = testCompletionExaminationService;

        private readonly CommandLineInterface commandLineInterface = commandLineInterface;

        public async Task RunTestAsync(TestMode testMode, int mutationEventCount)
        {
            if (mutationEventCount <= 0)
            {
                throw new ArgumentException("Mutation event count value must be greater than zero.", nameof(mutationEventCount));
            }

            var testId = this.GetPrimaryKey();

            await this.GetStreamProvider("TestStream")
                .GetStream<int>(StreamId.Create("ns", testId))
                .SubscribeAsync(this);

            commandLineInterface.WriteConsumerLogMessage("Handling completion of subscription by enqueuing producer mutation.");

            var producer = this.GrainFactory.GetGrain<IProducerGrain>(testId);

            Task.Run(async () =>
            {
                commandLineInterface.WriteConsumerLogMessage("Invoking producer mutation for {0} mutation events.", mutationEventCount);

                await producer.MutateStateAsync(mutationEventCount);
            });

            if (testMode == TestMode.Fixed)
            {
                commandLineInterface.WriteConsumerLogMessage("Enqueuing getting initial state on next turn as test mode is [green]fixed[/].");

                // TODO: is this the correct way of doing this - we should use the IActionInvoker system in practice?
                this.GrainContext.Scheduler.QueueAction(async _ =>
                    {
                        commandLineInterface.WriteConsumerLogMessage("Beginning getting initial state on new turn.");

                        await this.GetAndReportInitialState();
                    },
                    null!);

                return;
            }

            commandLineInterface.WriteConsumerLogMessage("Getting initial state immediately as test mode is [red]broken[/].");

            await this.GetAndReportInitialState();
        }

        public Task OnNextAsync(int item, StreamSequenceToken? token = null)
        {
            var testId = this.GetPrimaryKey();

            commandLineInterface.WriteConsumerLogMessage("Handling receival of event: [green]{0}[/] by reporting.", item);

            this.testCompletionExaminationService.ReportObservedMutationEvent(testId, item);

            return Task.CompletedTask;
        }

        public Task OnCompletedAsync()
        {
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception exception)
        {
            commandLineInterface.WriteConsumerErrorMessage("Got error:", exception);

            return Task.CompletedTask;
        }

        private async Task GetAndReportInitialState()
        {
            var testId = this.GetPrimaryKey();

            var state = await stateStore.GetStateAsync(testId);

            commandLineInterface.WriteConsumerLogMessage("Reporting initial retrieved state as: [green]{0}[/].", state?.ToString() ?? "<none>");

            this.testCompletionExaminationService.ReportStateRetrieved(testId, state);
        }
    }
}
