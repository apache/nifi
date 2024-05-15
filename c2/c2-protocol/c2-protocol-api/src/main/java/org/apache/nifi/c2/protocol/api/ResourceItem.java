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

package org.apache.nifi.c2.protocol.api;

import io.swagger.v3.oas.annotations.media.Schema;
import java.io.Serializable;
import java.util.Objects;

public class ResourceItem implements Serializable {

    private static final long serialVersionUID = 1L;

    private String resourceId;
    private String resourceName;
    private ResourceType resourceType;
    private String resourcePath;
    private String digest;
    private String hashType;
    private String url;

    @Schema(description = "The identifier of the resource to be synced")
    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    @Schema(description = "The name of the asset to be synced")
    public String getResourceName() {
        return resourceName;
    }

    public void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }

    @Schema(description = "The type of the asset to be synced")
    public ResourceType getResourceType() {
        return resourceType;
    }

    public void setResourceType(ResourceType resourceType) {
        this.resourceType = resourceType;
    }

    @Schema(description = "The relative path of the asset on the agent")
    public String getResourcePath() {
        return resourcePath;
    }

    public void setResourcePath(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    @Schema(description = "The checksum hash value calculated for the particular asset")
    public String getDigest() {
        return digest;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }

    @Schema(description = "The type of the hashing algorithm used to calculate the checksum")
    public String getHashType() {
        return hashType;
    }

    public void setHashType(String hashType) {
        this.hashType = hashType;
    }

    @Schema(description = "The relative url of the asset, from where the agent can download from")
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceItem that = (ResourceItem) o;
        return Objects.equals(resourceId, that.resourceId) && Objects.equals(resourceName, that.resourceName) && Objects.equals(resourceType, that.resourceType)
            && Objects.equals(resourcePath, that.resourcePath) && Objects.equals(digest, that.digest) && Objects.equals(hashType, that.hashType)
            && Objects.equals(url, that.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceId, resourceName, resourceType, resourcePath, digest, hashType, url);
    }

    @Override
    public String toString() {
        return "ResourceItem{" +
            "resourceId='" + resourceId + '\'' +
            ", resourceName='" + resourceName + '\'' +
            ", resourceType=" + resourceType +
            ", resourcePath='" + resourcePath + '\'' +
            ", digest='" + digest + '\'' +
            ", hashType='" + hashType + '\'' +
            ", url='" + url + '\'' +
            '}';
    }
}

