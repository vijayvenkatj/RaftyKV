/*
Package store has the implementation of the Store struct which handles the following:
  - In-memory key-value storage
  - Apply entries to the store with persistence using WAL
  - Replication to Followers

Replication:

  - For every PUT/DELETE operation, The Store first creates a LogEntry and appends to the WAL.

  - Then, We send a Replicate request to the follower nodes with (commitIndex -> currentIndex) log entries.

  - Once the majority of followers acknowledge the replication, we update the commitIndex and apply the log entries to the in-memory store.

  - If majority is not recognised, we return an error to the client.
*/
package store
