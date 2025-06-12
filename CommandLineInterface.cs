namespace OrleansMissingEvents
{
    using Spectre.Console;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    internal class CommandLineInterface
    {
        public void Initialize()
        {
            AnsiConsole.Clear();
        }

        public async Task<TestAction> PromptForNextTestActionAsync()
        {
            var testActionsByChoice = new Dictionary<string, TestAction>
            {
                {"[red]Run test as broken[/]", TestAction.RunBroken},
                {"[green]Run test as fixed[/]", TestAction.RunFixed},
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
            AnsiConsole.MarkupLine($"PRODUCER: {message}", args);
        }

        public void WriteConsumerLogMessage(string message, params object[] args)
        {
            AnsiConsole.MarkupLine($"[yellow]CONSUMER: {message}[/]", args);
        }

        public void WriteConsumerErrorMessage(string message, Exception exception, params object[] args)
        {
            AnsiConsole.MarkupLine($"[red]CONSUMER: {message}[/]", args);
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
