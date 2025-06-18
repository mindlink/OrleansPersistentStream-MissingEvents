namespace OrleansMissingEvents.TestHarness;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OrleansMissingEvents.ReliableMemoryStreams;

internal static class Program
{
    private const int EnMasseTestCount = 100;

    private const int EnMasseTestProgressBatchSize = 10;

    private const int MutationEventCount = 40;

    private const int InitialStreamSettlingDelayMilliseconds = 1000;

    private const int TimeoutPerEventMilliseconds = 500;

    public static async Task Main()
    {
        var commandLineInterface = new CommandLineInterface();

        commandLineInterface.Initialize();

        var hostApplicationBuilder = Host.CreateApplicationBuilder();

        hostApplicationBuilder.UseOrleans(siloBuilder =>
        {
            siloBuilder
                .UseLocalhostClustering()
                .AddMemoryGrainStorage("PubSubStore")
                .AddMemoryStreams(StreamProviderNames.NonReliableMemoryStreamsStreamProviderName)
                .AddReliableMemoryStreams(StreamProviderNames.ReliableMemoryStreamsStreamProviderName)
                .AddOutgoingGrainCallFilter<DelayedSubscriptionOutgoingGrainCallFilter>();
        });

        hostApplicationBuilder.Logging.ClearProviders();
        hostApplicationBuilder.Logging.AddProvider(new CommandLineInterfaceLoggerProvider(commandLineInterface));
        hostApplicationBuilder.Logging.AddFilter("*", LogLevel.None);
        hostApplicationBuilder.Logging.AddFilter("OrleansMissingEvents.*", LogLevel.Debug);

        hostApplicationBuilder.Services.AddSingleton(commandLineInterface);

        var testCompletionExaminerService = new TestCompletionExaminationService(MutationEventCount - 1);
        hostApplicationBuilder.Services.AddSingleton(testCompletionExaminerService);

        hostApplicationBuilder.Services.AddSingleton<StateStore>();

        var host = hostApplicationBuilder.Build();

        await host.StartAsync();

        var grainFactory = host.Services.GetRequiredService<IGrainFactory>();

        NextTestAction nextTestAction;

        while ((nextTestAction = await commandLineInterface.PromptForNextTestActionAsync()) != NextTestAction.Exit)
        {
            var testMode = nextTestAction is NextTestAction.RunSingleBrokenTest or NextTestAction.RunEnMasseBrokenTests
                ? TestMode.Broken : TestMode.Fixed;

            var isEnMasseTest =
                nextTestAction is NextTestAction.RunEnMasseBrokenTests or NextTestAction.RunEnMasseFixedTests;

            commandLineInterface.SetBackgroundLogsMuted(isEnMasseTest);

            var numberOfTests = isEnMasseTest ? EnMasseTestCount : 1;

            var cancellationTokenSource = new CancellationTokenSource();

            var pendingTestTasks = Enumerable.Range(0, numberOfTests).Select(_ =>
                    RunTestAsync(testMode, grainFactory, testCompletionExaminerService,
                        cancellationTokenSource.Token))
                .ToList();

            while (pendingTestTasks.Any())
            {
                var completedTestTask = await Task.WhenAny(pendingTestTasks);

                if (completedTestTask.IsFaulted)
                {
                    commandLineInterface.WriteTestFailMessage(
                        "A test failed as the operation timed out.");

                    cancellationTokenSource.Cancel();
                    break;
                }

                pendingTestTasks.Remove(completedTestTask);

                var testResults = completedTestTask.Result;

                if (testResults.TestStatus == TestStatus.Fail)
                {
                    commandLineInterface.WriteTestFailMessage(
                        "Test failed as '{0}' with calculated effective state {1}, retrieved state {2}, and observed events {3}.",
                        testResults.Description,
                        testResults.EffectiveState?.ToString() ?? "<none>",
                        testResults.RetrievedState?.ToString() ?? "<none>",
                        string.Join(", ", testResults.ObservedEvents));

                    cancellationTokenSource.Cancel();
                    break;
                }

                if (!pendingTestTasks.Any())
                {
                    if (numberOfTests == 1)
                    {
                        commandLineInterface.WriteTestPassMessage(
                            "Test passed as '{0}' with calculated effective state {1} from retrieved state {2} and observed events {3}.",
                            testResults.Description,
                            testResults.EffectiveState?.ToString() ?? "<none>",
                            testResults.RetrievedState?.ToString() ?? "<none>",
                            string.Join(", ", testResults.ObservedEvents));
                    }
                    else
                    {
                        commandLineInterface.WriteTestPassMessage(
                            "All '{0}' tests have passed. The last test passed as '{1}' with calculated effective state {2} from retrieved state {3} and observed events {4}.",
                            numberOfTests,
                            testResults.Description,
                            testResults.EffectiveState?.ToString() ?? "<none>",
                            testResults.RetrievedState?.ToString() ?? "<none>",
                            string.Join(", ", testResults.ObservedEvents));
                    }

                    break;
                }

                if (pendingTestTasks.Count % EnMasseTestProgressBatchSize == 0)
                {
                    commandLineInterface.WriteTestPassMessage(
                        "Test '{0}' of '{1}' has passed as '{2}' with calculated effective state {3} from retrieved state {4} and observed events {5}.",
                        numberOfTests - pendingTestTasks.Count,
                        numberOfTests,
                        testResults.Description,
                        testResults.EffectiveState?.ToString() ?? "<none>",
                        testResults.RetrievedState?.ToString() ?? "<none>",
                        string.Join(", ", testResults.ObservedEvents));
                }
            }

            commandLineInterface.WriteLine();
        }
    }

    private static async Task<TestResults> RunTestAsync(
        TestMode testMode, IGrainFactory grainFactory, TestCompletionExaminationService testCompletionExaminationService, CancellationToken cancellationToken)
    {
        var testId = Guid.NewGuid();

        var producerGrain = grainFactory.GetGrain<IProducerGrain>(testId);
        var consumerGrain = grainFactory.GetGrain<IConsumerGrain>(testId);

        var streamProviderName = testMode == TestMode.Broken
            ? StreamProviderNames.NonReliableMemoryStreamsStreamProviderName
            : StreamProviderNames.ReliableMemoryStreamsStreamProviderName;

        await producerGrain.WakeUpStreamAsync(streamProviderName); // Ensure stream is initialized in PersistentStreamPullingAgent.

        await Task.Delay(InitialStreamSettlingDelayMilliseconds, cancellationToken); // Wait for that initial event from the WakeUpStream call to settle.

        await consumerGrain.RunTestAsync(streamProviderName, MutationEventCount);

        return await testCompletionExaminationService.AwaitTestCompletionAsync(testId).WaitAsync(
            TimeSpan.FromMilliseconds(MutationEventCount * TimeoutPerEventMilliseconds), cancellationToken);
    }
}