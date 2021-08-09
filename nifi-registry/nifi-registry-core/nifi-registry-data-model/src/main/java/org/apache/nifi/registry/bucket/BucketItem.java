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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.registry.authorization.Permissions;
import org.apache.nifi.registry.link.LinkableEntity;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Objects;

@ApiModel
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

    @ApiModelProperty(value = "An ID to uniquely identify this object.", readOnly = true)
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @ApiModelProperty(value = "The name of the item.", required = true)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty("A description of the item.")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty(value = "The identifier of the bucket this items belongs to. This cannot be changed after the item is created.", required = true)
    public String getBucketIdentifier() {
        return bucketIdentifier;
    }

    public void setBucketIdentifier(String bucketIdentifier) {
        this.bucketIdentifier = bucketIdentifier;
    }

    @ApiModelProperty(value = "The name of the bucket this items belongs to.", readOnly = true)
    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    @ApiModelProperty(value = "The timestamp of when the item was created, as milliseconds since epoch.", readOnly = true)
    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(long createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    @ApiModelProperty(value = "The timestamp of when the item was last modified, as milliseconds since epoch.", readOnly = true)
    public long getModifiedTimestamp() {
        return modifiedTimestamp;
    }

    public void setModifiedTimestamp(long modifiedTimestamp) {
        this.modifiedTimestamp = modifiedTimestamp;
    }

    @ApiModelProperty(value = "The type of item.", required = true)
    public BucketItemType getType() {
        return type;
    }

    @ApiModelProperty(value = "The access that the current user has to the bucket containing this item.", readOnly = true)
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
