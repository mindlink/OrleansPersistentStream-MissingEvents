namespace OrleansMissingEvents
{
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;

    internal class Program
    {
        private const int MutationEventCount = 20;

        static async Task Main()
        {
            var commandLineInterface = new CommandLineInterface();

            commandLineInterface.Initialize();

            var hostApplicationBuilder = Host.CreateApplicationBuilder();

            hostApplicationBuilder.UseOrleans(siloBuilder =>
            {
                siloBuilder
                    .UseLocalhostClustering()
                    .AddMemoryGrainStorage("PubSubStore")
                    .AddMemoryStreams("TestStream");
            });

            hostApplicationBuilder.Logging.ClearProviders();

            hostApplicationBuilder.Services.AddSingleton(commandLineInterface);

            var testCompletionExaminerService = new TestCompletionExaminationService(MutationEventCount - 1);

            hostApplicationBuilder.Services.AddSingleton<StateStore>();
            hostApplicationBuilder.Services.AddSingleton(testCompletionExaminerService);

            var host = hostApplicationBuilder.Build();

            await host.StartAsync();

            var grainFactory = host.Services.GetRequiredService<IGrainFactory>();

            TestAction nextTestAction;

            while ((nextTestAction = await commandLineInterface.PromptForNextTestActionAsync()) != TestAction.Exit)
            {
                var testMode = nextTestAction == TestAction.RunBroken ? TestMode.Broken : TestMode.Fixed;

                var testId = Guid.NewGuid();

                var producerGrain = grainFactory.GetGrain<IProducerGrain>(testId);
                var consumerGrain = grainFactory.GetGrain<IConsumerGrain>(testId);

                await producerGrain.WakeUpStreamAsync(); // Ensure stream is initialized in PersistentStreamPullingAgent.

                await Task.Delay(500); // Wait for that initial event from the WakeUpStream call to settle.

                await consumerGrain.RunTestAsync(testMode, MutationEventCount);

                try
                {
                    var testResults = await testCompletionExaminerService.AwaitTestCompletion(testId).WaitAsync(TimeSpan.FromSeconds(MutationEventCount * 0.5));

                    if (testResults.TestStatus == TestStatus.Pass)
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
                        commandLineInterface.WriteTestFailMessage(
                            "Test failed as '{0}' with calculated effective state {1}, retrieved state {2}, and observed events {3}.",
                            testResults.Description,
                            testResults.EffectiveState?.ToString() ?? "<none>",
                            testResults.RetrievedState?.ToString() ?? "<none>",
                            string.Join(", ", testResults.ObservedEvents));
                    }
                }
                catch (TimeoutException)
                {
                    commandLineInterface.WriteTestFailMessage(
                        "Test failed as the operation timed out.");
                }

                commandLineInterface.WriteLine();
            }
        }
    }
}
