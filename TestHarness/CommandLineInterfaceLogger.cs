namespace OrleansMissingEvents.TestHarness;

using Microsoft.Extensions.Logging;

internal sealed class CommandLineInterfaceLogger(
    string name,
    CommandLineInterface commandLineInterface) : ILogger
{
    private readonly string name = name.Substring(name.LastIndexOf(".", StringComparison.Ordinal) + 1);

    private readonly CommandLineInterface commandLineInterface = commandLineInterface;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (logLevel == LogLevel.Warning)
        {
            this.commandLineInterface.WriteSystemWarningLogMessage(name, formatter(state, exception));

            return;
        }

        this.commandLineInterface.WriteSystemDebugLogMessage(name, formatter(state, exception));
    }

    public bool IsEnabled(LogLevel logLevel) => true;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
}