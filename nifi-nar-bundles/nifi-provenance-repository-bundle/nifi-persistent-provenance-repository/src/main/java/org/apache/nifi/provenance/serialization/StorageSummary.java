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

package org.apache.nifi.provenance.serialization;

import java.util.Optional;

public class StorageSummary {
    private final long eventId;
    private final String storageLocation;
    private final String partitionName;
    private final Integer blockIndex;
    private final long serializedLength;
    private final long bytesWritten;

    public StorageSummary(final long eventId, final String storageLocation, final Integer blockIndex, final long serializedLength, final long bytesWritten) {
        this(eventId, storageLocation, null, blockIndex, serializedLength, bytesWritten);
    }

    public StorageSummary(final long eventId, final String storageLocation, final String partitionName,
        final Integer blockIndex, final long serializedLength, final long bytesWritten) {
        this.eventId = eventId;
        this.storageLocation = storageLocation;
        this.partitionName = partitionName;
        this.blockIndex = blockIndex;
        this.serializedLength = serializedLength;
        this.bytesWritten = bytesWritten;
    }

    public long getEventId() {
        return eventId;
    }

    public String getStorageLocation() {
        return storageLocation;
    }

    public Optional<String> getPartitionName() {
        return Optional.ofNullable(partitionName);
    }

    public Integer getBlockIndex() {
        return blockIndex;
    }

    public long getSerializedLength() {
        return serializedLength;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    @Override
    public String toString() {
        return "StorageSummary[eventId=" + getEventId() + ", partition=" + getPartitionName().orElse(null) + ", location=" + getStorageLocation() + "]";
    }
}
