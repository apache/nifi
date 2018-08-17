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

package org.apache.nifi.wali;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface WriteAheadJournal<T> extends Closeable {

    JournalRecovery recoverRecords(Map<Object, T> recordMap, Set<String> swapLocations) throws IOException;

    /**
     * Updates the journal with the given set of records
     *
     * @param records the records to update
     * @param recordLookup a lookup that can be used to access the current value of a record, given its ID
     *
     * @throws IOException if unable to write to the underlying storage mechanism
     */
    void update(Collection<T> records, RecordLookup<T> recordLookup) throws IOException;

    void writeHeader() throws IOException;

    void fsync() throws IOException;

    /**
     * Returns information about what was written to the journal
     *
     * @return A JournalSummary indicating what was written to the journal
     * @throws IOException if unable to write to the underlying storage mechanism.
     */
    JournalSummary getSummary();

    /**
     * @return <code>true</code> if the journal is healthy and can be written to, <code>false</code> if either the journal has been closed or is poisoned
     */
    boolean isHealthy();

    /**
     * Destroys any resources that the journal occupies
     */
    void dispose();
}
