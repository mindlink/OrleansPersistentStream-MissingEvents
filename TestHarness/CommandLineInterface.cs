namespace OrleansMissingEvents.TestHarness;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Spectre.Console;

internal sealed class CommandLineInterface
{
    private bool areBackgroundLogsMuted;

    public void Initialize()
    {
        AnsiConsole.Clear();
    }

    public void SetBackgroundLogsMuted(bool areBackgroundLogsMuted)
    {
        this.areBackgroundLogsMuted = areBackgroundLogsMuted; ;
    }

    public async Task<NextTestAction> PromptForNextTestActionAsync()
    {
        var testActionsByChoice = new Dictionary<string, NextTestAction>
        {
            {"[red]Run single test as broken[/]", NextTestAction.RunSingleBrokenTest},
            {"[green]Run test as fixed[/]", NextTestAction.RunSingleFixedTest},
            {"[red]Run en-masse tests as broken[/]", NextTestAction.RunEnMasseBrokenTests},
            {"[green]Run en-masse tests as fixed[/]", NextTestAction.RunEnMasseFixedTests},
            {"[yellow]Exit[/]", NextTestAction.Exit}
        };

        var selectedTestActionChoice = await AnsiConsole.PromptAsync(
            new SelectionPrompt<string>()
                .Title("Please select action for next test:")
                .AddChoices(testActionsByChoice.Keys));

        return testActionsByChoice[selectedTestActionChoice];
    }

    public void WriteProducerLogMessage(string message, params object[] args)
    {
        if (this.areBackgroundLogsMuted)
        {
            return;
        }

        AnsiConsole.MarkupLine($"PRODUCER: {message}", args);
    }

    public void WriteConsumerLogMessage(string message, params object[] args)
    {
        if (this.areBackgroundLogsMuted)
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

    public void WriteSystemWarningLogMessage(string sourceName, string message)
    {
        AnsiConsole.MarkupLine($"[yellow]{sourceName.ToUpper()}: {Markup.Escape(message)}[/]");
    }

    public void WriteSystemDebugLogMessage(string sourceName, string message)
    {
        if (this.areBackgroundLogsMuted)
        {
            return;
        }

        AnsiConsole.MarkupLine($"[blue]{sourceName.ToUpper()}: {Markup.Escape(message)}[/]");
    }
}