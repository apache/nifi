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
package org.apache.nifi.web;

import org.apache.nifi.connectable.ConnectableType;

import java.util.Objects;

public final class RemovedConnectionDescriptor {
    private final String connectionInstanceId;
    private final String connectionVersionedId;
    private final String containingProcessGroupId;
    private final String sourceInstanceId;
    private final String sourceVersionedId;
    private final String sourceProcessGroupId;
    private final ConnectableType sourceType;
    private final String destinationInstanceId;
    private final String destinationVersionedId;
    private final String destinationProcessGroupId;
    private final ConnectableType destinationType;
    private final RemovalReason removalReason;

    public RemovedConnectionDescriptor(final String connectionInstanceId, final String connectionVersionedId,
                                       final String containingProcessGroupId,
                                       final String sourceInstanceId, final String sourceVersionedId, final String sourceProcessGroupId,
                                       final ConnectableType sourceType,
                                       final String destinationInstanceId, final String destinationVersionedId,
                                       final String destinationProcessGroupId, final ConnectableType destinationType,
                                       final RemovalReason removalReason) {
        this.connectionInstanceId = connectionInstanceId;
        this.connectionVersionedId = connectionVersionedId;
        this.containingProcessGroupId = containingProcessGroupId;
        this.sourceInstanceId = sourceInstanceId;
        this.sourceVersionedId = sourceVersionedId;
        this.sourceProcessGroupId = sourceProcessGroupId;
        this.sourceType = sourceType;
        this.destinationInstanceId = destinationInstanceId;
        this.destinationVersionedId = destinationVersionedId;
        this.destinationProcessGroupId = destinationProcessGroupId;
        this.destinationType = destinationType;
        this.removalReason = removalReason;
    }

    public String getConnectionInstanceId() {
        return connectionInstanceId;
    }

    public String getConnectionVersionedId() {
        return connectionVersionedId;
    }

    public String getContainingProcessGroupId() {
        return containingProcessGroupId;
    }

    public String getSourceInstanceId() {
        return sourceInstanceId;
    }

    public String getSourceVersionedId() {
        return sourceVersionedId;
    }

    public String getSourceProcessGroupId() {
        return sourceProcessGroupId;
    }

    public ConnectableType getSourceType() {
        return sourceType;
    }

    public String getDestinationInstanceId() {
        return destinationInstanceId;
    }

    public String getDestinationVersionedId() {
        return destinationVersionedId;
    }

    public String getDestinationProcessGroupId() {
        return destinationProcessGroupId;
    }

    public ConnectableType getDestinationType() {
        return destinationType;
    }

    public RemovalReason getRemovalReason() {
        return removalReason;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof final RemovedConnectionDescriptor other)) {
            return false;
        }

        return Objects.equals(connectionInstanceId, other.connectionInstanceId)
                && Objects.equals(connectionVersionedId, other.connectionVersionedId)
                && Objects.equals(containingProcessGroupId, other.containingProcessGroupId)
                && Objects.equals(sourceInstanceId, other.sourceInstanceId)
                && Objects.equals(sourceVersionedId, other.sourceVersionedId)
                && Objects.equals(sourceProcessGroupId, other.sourceProcessGroupId)
                && sourceType == other.sourceType
                && Objects.equals(destinationInstanceId, other.destinationInstanceId)
                && Objects.equals(destinationVersionedId, other.destinationVersionedId)
                && Objects.equals(destinationProcessGroupId, other.destinationProcessGroupId)
                && destinationType == other.destinationType
                && removalReason == other.removalReason;
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionInstanceId, connectionVersionedId, containingProcessGroupId,
                sourceInstanceId, sourceVersionedId, sourceProcessGroupId, sourceType,
                destinationInstanceId, destinationVersionedId, destinationProcessGroupId, destinationType,
                removalReason);
    }
}
