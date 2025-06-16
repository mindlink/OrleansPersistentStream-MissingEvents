namespace OrleansMissingEvents.TestHarness
{
    using Spectre.Console;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    internal class CommandLineInterface
    {
        private bool areProducerAndConsumerMuted;

        public void Initialize()
        {
            AnsiConsole.Clear();
        }

        public void SetProducerAndConsumerMuted(bool areProducerAndConsumerMuted)
        {
            this.areProducerAndConsumerMuted = areProducerAndConsumerMuted; ;
        }

        public async Task<TestAction> PromptForNextTestActionAsync()
        {
            var testActionsByChoice = new Dictionary<string, TestAction>
            {
                {"[red]Run single test as broken[/]", TestAction.RunSingleBrokenTest},
                {"[green]Run test as fixed[/]", TestAction.RunSingleFixedTest},
                {"[red]Run en-masse tests as broken[/]", TestAction.RunEnMasseBrokenTests},
                {"[green]Run en-masse tests as fixed[/]", TestAction.RunEnMasseFixedTests},
                {"[yellow]Exit[/]", TestAction.Exit}
            };

            var selectedTestActionChoice = await AnsiConsole.PromptAsync(
                new SelectionPrompt<string>()
                    .Title("Please select action for next test:")
                    .AddChoices(testActionsByChoice.Keys));

            return testActionsByChoice[selectedTestActionChoice];
        }

        public void WriteProducerLogMessage(string message, params object[] args)
        {
            if (this.areProducerAndConsumerMuted)
            {
                return;
            }

            AnsiConsole.MarkupLine($"PRODUCER: {message}", args);
        }

        public void WriteConsumerLogMessage(string message, params object[] args)
        {
            if (this.areProducerAndConsumerMuted)
            {
                return;
            }

            AnsiConsole.MarkupLine($"[yellow]CONSUMER: {message}[/]", args);
        }

        public void WriteConsumerErrorMessage(string message, Exception? exception, params object[] args)
        {
            AnsiConsole.MarkupLine($"[red]CONSUMER: {message}[/]", args);

            if (exception == null)
            {
                return;
            }

            AnsiConsole.WriteException(exception);
        }

        public void WriteTestPassMessage(string message, params object[] args)
        {
            AnsiConsole.MarkupLine($"[default on green]PASS: {message}[/]", args);
        }

        public void WriteTestFailMessage(string message, params object[] args)
        {
            AnsiConsole.MarkupLine($"[white on red]FAIL: {message}[/]", args);
        }

        public void WriteLine()
        {
            AnsiConsole.WriteLine();
        }
    }
}
