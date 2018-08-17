/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wali;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * <p>
 * A WriteAheadRepository is used to persist state that is otherwise kept
 * in-memory. The Repository does not provide any query capability except to
 * allow the data to be recovered upon restart of the system.
 * </p>
 *
 * <p>
 * A WriteAheadRepository operates by writing every update to an Edit Log. On
 * restart, the data can be recovered by replaying all of the updates that are
 * found in the Edit Log. This can, however, eventually result in very large
 * Edit Logs, which can both take up massive amounts of disk space and take a
 * long time to recover. In order to prevent this, the Repository provides a
 * Checkpointing capability. This allows the current in-memory state of the
 * Repository to be flushed to disk and the Edit Log to be deleted, thereby
 * compacting the amount of space required to store the Repository. After a
 * Checkpoint is performed, modifications are again written to an Edit Log. At
 * this point, when the system is to be restored, it is restored by first
 * loading the Checkpointed version of the Repository and then replaying the
 * Edit Log.
 * </p>
 *
 * <p>
 * All implementations of <code>WriteAheadRepository</code> use one or more
 * partitions to manage their Edit Logs. An implementation may require exactly
 * one partition or may allow many partitions.
 * </p>
 *
 * @param <T> the type of Record this repository is for
 */
public interface WriteAheadRepository<T> {

    /**
     * <p>
     * Updates the repository with the specified Records. The Collection must
     * not contain multiple records with the same ID
     * </p>
     *
     * @param records the records to update
     * @param forceSync specifies whether or not the Repository forces the data
     * to be flushed to disk. If false, the data may be stored in Operating
     * System buffers, which improves performance but could cause loss of data
     * if power is lost or the Operating System crashes
     * @throws IOException if failure to update repo
     * @throws IllegalArgumentException if multiple records within the given
     * Collection have the same ID, as specified by {@link Record#getId()}
     * method
     *
     * @return the index of the Partition that performed the update
     */
    int update(Collection<T> records, boolean forceSync) throws IOException;

    /**
     * <p>
     * Recovers all records from the persisted state. This method must be called
     * before any updates are issued to the Repository.
     * </p>
     *
     * @return recovered records
     * @throws IOException if failure to read from repo
     * @throws IllegalStateException if any updates have been issued against
     * this Repository before this method is invoked
     */
    Collection<T> recoverRecords() throws IOException;

    /**
     * <p>
     * Recovers all External Swap locations that were persisted. If this method
     * is to be called, it must be called AFTER {@link #recoverRecords()} and
     * BEFORE {@link #update(Collection, boolean)}}.
     * </p>
     *
     * @return swap location
     * @throws IOException if failure reading swap locations
     */
    Set<String> getRecoveredSwapLocations() throws IOException;

    /**
     * <p>
     * Compacts the contents of the Repository so that rather than having a
     * Snapshot and an Edit Log indicating many Updates to the Snapshot, the
     * Snapshot is updated to contain the current state of the Repository, and
     * the edit log is purged.
     * </p>
     *
     *
     * @return the number of records that were written to the new snapshot
     * @throws java.io.IOException if failure during checkpoint
     */
    int checkpoint() throws IOException;

    /**
     * <p>
     * Causes the repository to checkpoint and then close any open resources.
     * </p>
     *
     * @throws IOException if failure to shutdown cleanly
     */
    void shutdown() throws IOException;
}
