package cdc_test

import "testing"

func TestUnsupportedSQLIsRejected(t *testing.T) {
	Given(APostgresDatabase(t)).
		And(AJetStream(t)).
		When(AnUnsupportedWriteIsAttempted(t)).
		Then(TheWriteIsRejectedAndNothingIsStored(t))
}

func TestPostgresCommitReplicatesToSQLite(t *testing.T) {
	Given(AReplicatingPair(t)).
		When(PostgresCommitsInsertAndUpdate(t)).
		Then(SQLiteShowsTheCommittedPostgresRows(t)).
		And(TheStreamHasOnlyTheCommittedPostgresChanges(t))
}

func TestRolledBackDeleteIsNotReplicated(t *testing.T) {
	Given(AReplicatingPair(t)).
		And(BothSidesShowTheCommittedPair(t)).
		When(PostgresRollsBackADelete(t)).
		Then(SQLiteShowsTheCommittedPostgresRows(t)).
		And(TheStreamHasOnlyTheCommittedPostgresChanges(t))
}

func TestDuplicateMessageIDDoesNotGrowTheStream(t *testing.T) {
	Given(AReplicatingPair(t)).
		And(AnInsertHasBeenPublished(t)).
		When(TheFirstEventIsRepublishedWithTheSameMsgID(t)).
		Then(TheStreamDoesNotGrow(t))
}

func TestPostgresDeleteReplicatesToSQLite(t *testing.T) {
	Given(AReplicatingPair(t)).
		And(BothSidesShowTheCommittedPair(t)).
		When(PostgresCommitsADelete(t)).
		Then(SQLiteNoLongerHasTheDeletedRow(t))
}

func TestReplayingTheStreamRestoresSQLite(t *testing.T) {
	Given(AReplicatingPair(t)).
		And(BothSidesShowTheRowLeftAfterADelete(t)).
		When(SQLiteIsWipedAndTheStreamIsReplayed(t)).
		Then(SQLiteMatchesTheCommittedPostgresState(t))
}

func TestSQLiteCommitReplicatesToPostgres(t *testing.T) {
	Given(AReplicatingPair(t)).
		And(BothSidesShowTheRowLeftAfterADelete(t)).
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
