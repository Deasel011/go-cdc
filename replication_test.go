package cdc_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestReplication(t *testing.T) {
	suite.Run(t, new(ReplicationSuite))
}

func (s *ReplicationSuite) TestUnsupportedSQLIsRejected() {
	s.When(s.AnUnsupportedWriteIsAttempted).
		Then(s.TheWriteIsRejectedAndNothingIsStored)
}

func (s *ReplicationSuite) TestPostgresCommitReplicatesToSQLite() {
	s.Given(s.ReplicationRunningBothWays).
		When(s.PostgresCommitsInsertAndUpdate).
		Then(s.SQLiteShowsTheCommittedPostgresRows).
		And(s.TheStreamHasOnlyTheCommittedPostgresChanges)
}

func (s *ReplicationSuite) TestRolledBackDeleteIsNotReplicated() {
	s.Given(s.ReplicationRunningBothWays).
		And(s.BothSidesShowTheCommittedPair).
		When(s.PostgresRollsBackADelete).
		Then(s.SQLiteShowsTheCommittedPostgresRows).
		And(s.TheStreamHasOnlyTheCommittedPostgresChanges)
}

func (s *ReplicationSuite) TestDuplicateMessageIDDoesNotGrowTheStream() {
	s.Given(s.ReplicationRunningBothWays).
		And(s.AnInsertHasBeenPublished).
		When(s.TheFirstEventIsRepublishedWithTheSameMsgID).
		Then(s.TheStreamDoesNotGrow)
}

func (s *ReplicationSuite) TestPostgresDeleteReplicatesToSQLite() {
	s.Given(s.ReplicationRunningBothWays).
		And(s.BothSidesShowTheCommittedPair).
		When(s.PostgresCommitsADelete).
		Then(s.SQLiteNoLongerHasTheDeletedRow)
}

func (s *ReplicationSuite) TestReplayingTheStreamRestoresSQLite() {
	s.Given(s.ReplicationRunningBothWays).
		And(s.BothSidesShowTheRowLeftAfterADelete).
		When(s.SQLiteIsWipedAndTheStreamIsReplayed).
		Then(s.SQLiteMatchesTheCommittedPostgresState)
}

func (s *ReplicationSuite) TestSQLiteCommitReplicatesToPostgres() {
	s.Given(s.ReplicationRunningBothWays).
		And(s.BothSidesShowTheRowLeftAfterADelete).
		When(s.SQLiteCommitsAnUpdateAndInsert).
		Then(s.PostgresShowsTheSQLiteChanges).
		And(s.NoEchoWasPublished)
}

func (s *ReplicationSuite) TestApplierRejectsMaliciousIdentifier() {
	s.When(s.AMaliciousEventIsPublished).
		Then(s.TheApplierStopsAndTheTableRemains)
}
