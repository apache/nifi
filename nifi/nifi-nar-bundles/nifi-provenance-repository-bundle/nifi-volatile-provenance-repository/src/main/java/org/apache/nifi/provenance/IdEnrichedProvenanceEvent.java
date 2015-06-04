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

package org.apache.nifi.provenance;

/**
 * Simple filter class to assign an arbitrary ID to a provenance event
 */
final class IdEnrichedProvenanceEvent extends FilterProvenanceEventRecord {

    private long eventId;

    public IdEnrichedProvenanceEvent(final ProvenanceEventRecord record) {
        super(record);
    }

    @Override
    public long getEventId() {
        return eventId;
    }

    public void setEventId(final long eventId) {
        this.eventId = eventId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + (int) (eventId ^ (eventId >>> 32));
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IdEnrichedProvenanceEvent other = (IdEnrichedProvenanceEvent) obj;
        if (eventId != other.eventId) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "IdEnrichedProvenanceEvent [eventId=" + this.eventId + ", record=" + super.record
                + "]";
    }
}
