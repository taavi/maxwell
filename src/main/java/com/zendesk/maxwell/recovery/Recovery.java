package com.zendesk.maxwell.recovery;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellMysqlConfig;
import com.zendesk.maxwell.monitoring.NoOpMetrics;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.BinlogConnectorReplicator;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.HeartbeatNotifier;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.zendesk.maxwell.recovery.Recovery.BinlogTemporalLocation.JUST_RIGHT;
import static com.zendesk.maxwell.recovery.Recovery.BinlogTemporalLocation.TOO_EARLY;
import static com.zendesk.maxwell.recovery.Recovery.BinlogTemporalLocation.TOO_LATE;

public class Recovery {
	static final Logger LOGGER = LoggerFactory.getLogger(Recovery.class);

	private final ConnectionPool replicationConnectionPool;
	private final RecoveryInfo recoveryInfo;
	private final long targetHeartbeat;
	private final MaxwellMysqlConfig replicationConfig;
	private final String maxwellDatabaseName;
	private final RecoverySchemaStore schemaStore;
	enum BinlogTemporalLocation { TOO_LATE, JUST_RIGHT, TOO_EARLY}

	private class BinlogDisposition {
		private final BinlogTemporalLocation whence;
		private final HeartbeatRowMap heartbeatRowMap;

		BinlogDisposition(BinlogTemporalLocation whence, HeartbeatRowMap heartbeatRowMap) {
			this.whence = whence;
			this.heartbeatRowMap = heartbeatRowMap;
		}
	}

	public Recovery(MaxwellMysqlConfig replicationConfig,
					String maxwellDatabaseName,
					ConnectionPool replicationConnectionPool,
					CaseSensitivity caseSensitivity,
					RecoveryInfo recoveryInfo) {
		this.replicationConfig = replicationConfig;
		this.replicationConnectionPool = replicationConnectionPool;
		this.recoveryInfo = recoveryInfo;
		this.targetHeartbeat = recoveryInfo.getHeartbeat();
		this.schemaStore = new RecoverySchemaStore(replicationConnectionPool, maxwellDatabaseName, caseSensitivity);
		this.maxwellDatabaseName = maxwellDatabaseName;
	}

	public HeartbeatRowMap recover() throws Exception {
		String recoveryMsg = String.format(
			"old-server-id: %d, position: %s",
			recoveryInfo.serverID,
			recoveryInfo.position
		);
		LOGGER.warn("attempting to recover from master-change: " + recoveryMsg);

		// 1. Get latest binlog (I mean…all is fine)
		// 2. Search that binlog for our heartbeat
		// 2a. If we find it, great!
		// 2b. If this binlog is too late, then go_backwards()
		// 2c. If this binlog is too early, then refresh/go_forwards()

		List<BinlogPosition> list = getBinlogInfo();
		BinlogPosition binlogPosition = list.get(list.size() - 1);
		Position position = Position.valueOf(binlogPosition, targetHeartbeat);

		LOGGER.debug("scanning latest binlog: " + binlogPosition);
		BinlogDisposition disposition = findHeartbeat(position);

		if (disposition.whence == JUST_RIGHT) {
			return logRecoveredHeartbeat(disposition);
		} else if (disposition.whence == TOO_LATE) {
			// Keep looking backwards
			for ( int i = list.size() - 2; i >= 0 && disposition.whence == TOO_LATE; i-- ) {
				binlogPosition = list.get(i);
				position = Position.valueOf(binlogPosition, targetHeartbeat);

				LOGGER.debug("scanning earlier binlog: " + binlogPosition);
				disposition = findHeartbeat(position);

				if (disposition.whence == JUST_RIGHT) {
					return logRecoveredHeartbeat(disposition);
				} else if (disposition.whence == TOO_EARLY) {
					// Well, we were looking backwards but decided this was "too early", which means
					// that we've missed the mark.
					LOGGER.warn(
						"Couldn't find heartbeat " + targetHeartbeat + " " +
						"in binlog " + position + ", and we didn't see the heartbeat in the " +
						"binlog after this one either." +
						"We are confused. Is this even the right replica?");
					return null;
				}
				// And let TOO_LATE fall through back to the for loop.
			}
		} else if (disposition.whence == TOO_EARLY) {
			// Two options:
			// 1) Replication hasn't caught up (is it a delay replica? or just catching up?)
			// 2) Corruption, and we're never going to find our heartbeat.
			// Welp. Let's just try again?
			// TODO: restart recovery from the point we got to!
			// Check to see if there are more binlogs? If so, we should at least re-try the current file from where we left off. Then should continue on to the next file, and repeat.
			for ( int retryCount = 0; retryCount < 10; retryCount++ ) {
				Thread.sleep(1000);
				BinlogPosition newlyFoundBinlogPosition;
				if (disposition.heartbeatRowMap != null) {
					newlyFoundBinlogPosition = disposition.heartbeatRowMap.getPosition().getBinlogPosition();
				} else {
					newlyFoundBinlogPosition = list.get(list.size() - 1);
				}
				int currentBinlogPosition = list.size() - 1;
				List<BinlogPosition> maybeMoreBinlogs = getBinlogInfo();
				// Replace the current head with where we left off
				list.set(currentBinlogPosition, newlyFoundBinlogPosition);
				// Splice the new ones on top of the old one
				for (BinlogPosition p: maybeMoreBinlogs) {
					if (p.newerThan(newlyFoundBinlogPosition)) {
						list.add(p);
					}
				}
				for ( int i = currentBinlogPosition; i < list.size(); i++ ) {
					binlogPosition = list.get(i);
					position = Position.valueOf(binlogPosition, targetHeartbeat);

					LOGGER.debug("scanning forward from binlog: " + binlogPosition);
					disposition = findHeartbeat(position);

					if (disposition.whence == JUST_RIGHT) {
						return logRecoveredHeartbeat(disposition);
					} else if (disposition.whence == TOO_LATE) {
						// Well, we were looking forwards but found this was "too late", which means
						// that we've missed the mark.
						LOGGER.warn(
							"Couldn't find heartbeat " + targetHeartbeat + " " +
							"in binlog " + position + ", and we didn't see the heartbeat in the " +
							"binlog before this one either." +
							"We are confused. Is this even the right replica?");
						return null;
					}
					// And let TOO_EARLY fall through to the for loop.
				}
				// Okay, so we tried looking forward and still didn't find what we were looking for.
				// I guess we can just try again.
			}
		}

		LOGGER.error("Could not recover from master-change: " + recoveryMsg);
		return null;
	}

	private HeartbeatRowMap logRecoveredHeartbeat(BinlogDisposition disposition) {
		LOGGER.warn("recovered new master position: " + disposition.heartbeatRowMap.getNextPosition());
		return disposition.heartbeatRowMap;
	}

	private BinlogDisposition findHeartbeat(Position position) throws Exception {
		Replicator replicator = new BinlogConnectorReplicator(
				this.schemaStore,
				null,
				null,
				replicationConfig,
				0L, // server-id of 0 activates "mysqlbinlog" behavior where the server will stop after each binlog
				maxwellDatabaseName,
				new NoOpMetrics(),
				position,
				true,
				recoveryInfo.clientID,
				new HeartbeatNotifier(),
				null,
				new RecoveryFilter(this.maxwellDatabaseName),
				new MaxwellOutputConfig(),
				0.25f // Default memory usage size, not used
		);

		HeartbeatRowMap lastRowSeen = null;
		replicator.startReplicator();
		for (RowMap row = replicator.getRow(); row != null ; row = replicator.getRow()) {
			if (!(row instanceof HeartbeatRowMap)) {
				continue;
			}
			HeartbeatRowMap heartbeatRow = (HeartbeatRowMap) row;
			final long lastHeartbeat = heartbeatRow.getPosition().getLastHeartbeatRead();
			if (lastHeartbeat == targetHeartbeat) {
				return new BinlogDisposition(JUST_RIGHT, heartbeatRow);
			} else if (lastHeartbeat > targetHeartbeat) {
				if (lastRowSeen == null) {
					// This is the FIRST heartbeat row we've seen in this file, and it's too big.
					// That means that the heartbeat we're looking for is almost assuredly in a
					// previous binlog file.
					return new BinlogDisposition(TOO_LATE, null);
				} else {
					// This is not the first heartbeat...but the previous one was too small
					// (otherwise we wouldn't be at this line of code).
					// This means that the heartbeat we want isn't where it should be! ERROR!
					throw new Exception(
						"Couldn't find heartbeat " + targetHeartbeat + " " +
						"in binlog " + position + ". " +
						"Found " + lastRowSeen.getPosition().getLastHeartbeatRead() +
						" and " + lastHeartbeat + " though. " +
						"We are confused. Is this even the right replica?");
				}
			}
			lastRowSeen = heartbeatRow;
		}
		// Okay, so we finished reading through the entire file...
		if (lastRowSeen == null) {
			// ...but we saw zero heartbeats. I guess it's a new binlog? Should we retry this one?
			// Should we try a previous one? I DON'T KNOW! Let's go with the v1.25.0 behaviour.
			return new BinlogDisposition(TOO_LATE, null);
		} else {
			return new BinlogDisposition(TOO_EARLY, lastRowSeen);
		}
	}

	/**
	 * fetch a list of binlog positions representing the start of each binlog file
	 *
	 * @return a list of binlog positions to attempt recovery at
	 * */

	private List<BinlogPosition> getBinlogInfo() throws SQLException {
		ArrayList<BinlogPosition> list = new ArrayList<>();
		try ( Connection c = replicationConnectionPool.getConnection() ) {
			ResultSet rs = c.createStatement().executeQuery("SHOW BINARY LOGS");
			while ( rs.next() ) {
				list.add(BinlogPosition.at(4, rs.getString("Log_name")));
			}
		}
		return list;
	}
}
