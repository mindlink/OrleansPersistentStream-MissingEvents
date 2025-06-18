namespace OrleansMissingEvents.TestHarness;

using System.Collections.Generic;

internal record TestResults(TestStatus TestStatus, string Description, int? EffectiveState, int? RetrievedState, IEnumerable<int> ObservedEvents);