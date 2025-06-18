namespace OrleansMissingEvents.TestHarness;

using Microsoft.Extensions.Logging;

internal sealed class CommandLineInterfaceLoggerProvider(CommandLineInterface commandLineInterface) : ILoggerProvider
{
    private readonly CommandLineInterface commandLineInterface = commandLineInterface;

    public void Dispose()
    {
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new CommandLineInterfaceLogger(categoryName, this.commandLineInterface);
    }
}