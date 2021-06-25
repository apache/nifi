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
package org.apache.nifi.registry.flow;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.registry.link.LinkableEntity;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import java.util.Objects;

/**
 * The metadata information about a VersionedFlowSnapshot. This class implements Comparable in order
 * to sort based on the snapshot version in ascending order.
 */
@ApiModel
public class VersionedFlowSnapshotMetadata extends LinkableEntity implements Comparable<VersionedFlowSnapshotMetadata> {

    @NotBlank
    private String bucketIdentifier;

    @NotBlank
    private String flowIdentifier;

    @Min(-1)
    private int version;

    @Min(1)
    private long timestamp;

    @NotBlank
    private String author;

    private String comments;


    @ApiModelProperty(value = "The identifier of the bucket this snapshot belongs to.", required = true)
    public String getBucketIdentifier() {
        return bucketIdentifier;
    }

    public void setBucketIdentifier(String bucketIdentifier) {
        this.bucketIdentifier = bucketIdentifier;
    }

    @ApiModelProperty(value = "The identifier of the flow this snapshot belongs to.", required = true)
    public String getFlowIdentifier() {
        return flowIdentifier;
    }

    public void setFlowIdentifier(String flowIdentifier) {
        this.flowIdentifier = flowIdentifier;
    }

    @ApiModelProperty(value = "The version of this snapshot of the flow.", required = true)
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @ApiModelProperty(value = "The timestamp when the flow was saved, as milliseconds since epoch.", readOnly = true)
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @ApiModelProperty(value = "The user that created this snapshot of the flow.", readOnly = true)
    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    @ApiModelProperty("The comments provided by the user when creating the snapshot.")
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    @Override
    public int compareTo(final VersionedFlowSnapshotMetadata o) {
        return o == null ? -1 : Integer.compare(version, o.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.flowIdentifier, Integer.valueOf(this.version));
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final VersionedFlowSnapshotMetadata other = (VersionedFlowSnapshotMetadata) obj;

        return Objects.equals(this.flowIdentifier, other.flowIdentifier)
                && Objects.equals(this.version, other.version);
    }
}
