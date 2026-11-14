# Purpose of the package
This package binds the database/sql golang package to a CDC mechanism so a database can be replicated elsewhere and abstracts the underlying relational database engine.

# Testing Strategy
Use Postgres as the source database. Use a Nats Jetstream as the CDC stream. Mirror and/or source this stream to re-apply the changes over into an SQLite database.
Make sure that the reverse also works.
Focus on integration tests rather than unit tests, using the least amnount of tests that will give a thorough coverage.

# Coding Stragegy
Use a Test First approach. 
Use a Given When Then test structure.
Create a X_testsuite_structure.go file to setup methods used by the tests themselves.
Write the tests with Given(ASetupMethodFromTheStructureFile).When(AnActionMethodFromTheStructureFile).Then(AnAssertMethodFromTheStructureFile), you can use .And() to add more context to a given, when or then portion of the test.
