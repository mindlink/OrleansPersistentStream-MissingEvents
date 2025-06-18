namespace OrleansMissingEvents.TestHarness;

internal enum NextTestAction
{
    RunSingleBrokenTest,

    RunSingleFixedTest,

    RunEnMasseBrokenTests,

    RunEnMasseFixedTests,

    Exit
}