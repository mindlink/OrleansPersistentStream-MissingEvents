namespace OrleansMissingEvents.TestHarness;

using System;
using Orleans.Runtime;
using Orleans.Streams;

internal sealed class ConsumerGrain(StateStore stateStore, TestCompletionExaminationService testCompletionExaminationService, CommandLineInterface commandLineInterface) : Grain, IConsumerGrain
{
    private readonly StateStore stateStore = stateStore;

    private readonly TestCompletionExaminationService testCompletionExaminationService = testCompletionExaminationService;

    private readonly CommandLineInterface commandLineInterface = commandLineInterface;

    public async Task RunTestAsync(string streamProviderName, int mutationEventCount)
    {
        if (mutationEventCount <= 0)
        {
            throw new ArgumentException("Mutation event count value must be greater than zero.", nameof(mutationEventCount));
        }

        var testId = this.GetPrimaryKey();

        var producer = GrainFactory.GetGrain<IProducerGrain>(testId);

        _ = Task.Run(async () =>
        {
            commandLineInterface.WriteConsumerLogMessage("Invoking producer mutation for {0} mutation events.", mutationEventCount);

            await producer.MutateStateAsync(mutationEventCount);
        });

        var asyncStream = this.GetStreamProvider(streamProviderName)
            .GetStream<int>(StreamId.Create("ns", testId));

        await asyncStream.SubscribeAsync(this);

        commandLineInterface.WriteConsumerLogMessage("Handling completion of reliable subscription by getting initial state immediately.");

        await GetAndReportInitialState();
    }

    public Task OnNextAsync(int item, StreamSequenceToken? token = null)
    {
        var testId = this.GetPrimaryKey();

        if (item == -1)
        {
            commandLineInterface.WriteConsumerErrorMessage("Ignoring initial item received on stream.", null!);

            return Task.CompletedTask;
        }

        commandLineInterface.WriteConsumerLogMessage("Handling receival of event '[green]{0}[/]' by reporting to test examiner.", item);

        testCompletionExaminationService.ReportObservedMutationEvent(testId, item);

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

        commandLineInterface.WriteConsumerLogMessage("Handling completion of initial retrieved state as '[green]{0}[/]' by reporting to test examiner.", state?.ToString() ?? "<none>");

        testCompletionExaminationService.ReportStateRetrieved(testId, state);
    }
}