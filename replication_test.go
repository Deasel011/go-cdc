package cdc_test

import "testing"

func TestChangesReplicateBothWays(t *testing.T) {
	Given(APostgresDatabase(t)).
		And(ASQLiteDatabase(t)).
		And(AJetStream(t)).
		And(ReplicationRunningBothWays(t)).
		When(AnUnsupportedWriteIsAttempted(t)).
		Then(TheWriteIsRejectedAndNothingIsStored(t)).
		When(PostgresCommitsInsertAndUpdate(t)).
		And(PostgresRollsBackADelete(t)).
		Then(SQLiteShowsTheCommittedPostgresRows(t)).
		And(TheRolledBackDeleteIsAbsentFromTheStream(t)).
		When(TheFirstEventIsRepublishedWithTheSameMsgID(t)).
		Then(TheStreamDoesNotGrow(t)).
		When(PostgresCommitsADelete(t)).
		Then(SQLiteNoLongerHasTheDeletedRow(t)).
		When(SQLiteIsWipedAndTheStreamIsReplayed(t)).
		Then(SQLiteMatchesTheCommittedPostgresState(t)).
		When(SQLiteCommitsAnUpdateAndInsert(t)).
		Then(PostgresShowsTheSQLiteChanges(t)).
		And(NoEchoWasPublished(t))
}

func TestApplierRejectsMaliciousIdentifier(t *testing.T) {
	Given(ASQLiteDatabase(t)).
		And(AJetStream(t)).
		When(AMaliciousEventIsPublished(t)).
		Then(TheApplierStopsAndTheTableRemains(t))
}
