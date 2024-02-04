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
package org.apache.nifi.registry.bucket;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Objects;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.apache.nifi.registry.authorization.Permissions;
import org.apache.nifi.registry.link.LinkableEntity;

public abstract class BucketItem extends LinkableEntity {

    @NotBlank
    private String identifier;

    @NotBlank
    private String name;

    private String description;

    @NotBlank
    private String bucketIdentifier;

    // read-only
    private String bucketName;

    @Min(1)
    private long createdTimestamp;

    @Min(1)
    private long modifiedTimestamp;

    @NotNull
    private final BucketItemType type;

    private Permissions permissions;


    public BucketItem(final BucketItemType type) {
        this.type = type;
    }

    @Schema(description = "An ID to uniquely identify this object.", accessMode = Schema.AccessMode.READ_ONLY)
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @Schema(description = "The name of the item.")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Schema(description = "A description of the item.")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Schema(description = "The identifier of the bucket this items belongs to. This cannot be changed after the item is created.")
    public String getBucketIdentifier() {
        return bucketIdentifier;
    }

    public void setBucketIdentifier(String bucketIdentifier) {
        this.bucketIdentifier = bucketIdentifier;
    }

    @Schema(description = "The name of the bucket this items belongs to.", accessMode = Schema.AccessMode.READ_ONLY)
    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    @Schema(description = "The timestamp of when the item was created, as milliseconds since epoch.", accessMode = Schema.AccessMode.READ_ONLY)
    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(long createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    @Schema(description = "The timestamp of when the item was last modified, as milliseconds since epoch.", accessMode = Schema.AccessMode.READ_ONLY)
    public long getModifiedTimestamp() {
        return modifiedTimestamp;
    }

    public void setModifiedTimestamp(long modifiedTimestamp) {
        this.modifiedTimestamp = modifiedTimestamp;
    }

    @Schema(description = "The type of item.")
    public BucketItemType getType() {
        return type;
    }

    @Schema(description = "The access that the current user has to the bucket containing this item.", accessMode = Schema.AccessMode.READ_ONLY)
    public Permissions getPermissions() {
        return permissions;
    }

    public void setPermissions(Permissions permissions) {
        this.permissions = permissions;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.getIdentifier());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final BucketItem other = (BucketItem) obj;
        return Objects.equals(this.getIdentifier(), other.getIdentifier());
    }
}
