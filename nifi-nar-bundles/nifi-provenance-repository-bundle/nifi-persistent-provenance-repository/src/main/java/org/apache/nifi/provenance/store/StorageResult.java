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

package org.apache.nifi.provenance.store;

import java.util.Collections;
import java.util.Map;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.serialization.StorageSummary;

public interface StorageResult {
    /**
     * @return a map of each Provenance Event Record to the location where it was stored
     */
    Map<ProvenanceEventRecord, StorageSummary> getStorageLocations();

    /**
     * Indicates whether or not the storage of events triggered the store to roll over
     * the storage location that it is storing data to
     *
     * @return <code>true</code> if the store rolled over to a new storage location, <code>false</code> otherwise
     */
    boolean triggeredRollover();

    /**
     * @return the number of events that were stored in the storage location that was rolled over, or
     *         <code>null</code> if no storage locations were rolled over.
     */
    Integer getEventsRolledOver();

    public static StorageResult EMPTY = new StorageResult() {
        @Override
        public Map<ProvenanceEventRecord, StorageSummary> getStorageLocations() {
            return Collections.emptyMap();
        }

        @Override
        public boolean triggeredRollover() {
            return false;
        }

        @Override
        public Integer getEventsRolledOver() {
            return null;
        }

        @Override
        public String toString() {
            return "StorageResult.EMPTY";
        }
    };
}
