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
import org.apache.nifi.registry.revision.entity.RevisableEntity;
import org.apache.nifi.registry.revision.entity.RevisionInfo;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Objects;

@XmlRootElement
@ApiModel
public class Bucket extends LinkableEntity implements RevisableEntity {

    @NotBlank
    private String identifier;

    @NotBlank
    private String name;

    @Min(1)
    private long createdTimestamp;

    private String description;

    private Boolean allowBundleRedeploy;

    private Boolean allowPublicRead;

    private Permissions permissions;

    private RevisionInfo revision;

    @ApiModelProperty(value = "An ID to uniquely identify this object.", readOnly = true)
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @ApiModelProperty(value = "The name of the bucket.", required = true)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty(value = "The timestamp of when the bucket was first created. This is set by the server at creation time.", readOnly = true)
    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(long createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    @ApiModelProperty("A description of the bucket.")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty("Indicates if this bucket allows the same version of an extension bundle to be redeployed and thus overwrite the existing artifact. By default this is false.")
    public Boolean isAllowBundleRedeploy() {
        return allowBundleRedeploy;
    }

    public void setAllowBundleRedeploy(final Boolean allowBundleRedeploy) {
        this.allowBundleRedeploy = allowBundleRedeploy;
    }

    @ApiModelProperty("Indicates if this bucket allows read access to unauthenticated anonymous users")
    public Boolean isAllowPublicRead() {
        return allowPublicRead;
    }

    public void setAllowPublicRead(final Boolean allowPublicRead) {
        this.allowPublicRead = allowPublicRead;
    }

    @ApiModelProperty(value = "The access that the current user has to this bucket.", readOnly = true)
    public Permissions getPermissions() {
        return permissions;
    }

    public void setPermissions(Permissions permissions) {
        this.permissions = permissions;
    }

    @ApiModelProperty(
            value = "The revision of this entity used for optimistic-locking during updates.",
            readOnly = true
    )
    @Override
    public RevisionInfo getRevision() {
        return revision;
    }

    @Override
    public void setRevision(RevisionInfo revision) {
        this.revision = revision;
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

        final Bucket other = (Bucket) obj;
        return Objects.equals(this.getIdentifier(), other.getIdentifier());
    }

}
