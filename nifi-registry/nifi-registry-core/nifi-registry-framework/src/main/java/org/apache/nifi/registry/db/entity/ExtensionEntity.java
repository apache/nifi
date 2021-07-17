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
package org.apache.nifi.registry.db.entity;

import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.component.manifest.ExtensionType;

import java.util.Set;

public class ExtensionEntity {

    private String id;
    private String bundleVersionId;
    private String name;
    private String displayName;
    private ExtensionType extensionType;

    // serialized content of Extension
    private String content;

    // populated during creation if provided, but typically won't be populated on retrieval
    private String additionalDetails;

    // read-only to let consumers know there are additional details that have not be returned, but can be retrieved later
    private boolean hasAdditionalDetails;

    // populated during creation to insert into child tables, but won't be populated on retrieval b/c the
    // content field contains all of this info and will be deserialized into the full extension
    private Set<String> tags;
    private Set<ExtensionProvidedServiceApiEntity> providedServiceApis;
    private Set<ExtensionRestrictionEntity> restrictions;

    // read-only - populated on retrieval only by joining with additional tables
    private String bucketId;
    private String bucketName;
    private String bundleId;
    private String groupId;
    private String artifactId;
    private String version;
    private String systemApiVersion;
    private BundleType bundleType;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getBundleVersionId() {
        return bundleVersionId;
    }

    public void setBundleVersionId(String bundleVersionId) {
        this.bundleVersionId = bundleVersionId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public ExtensionType getExtensionType() {
        return extensionType;
    }

    public void setExtensionType(ExtensionType extensionType) {
        this.extensionType = extensionType;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getAdditionalDetails() {
        return additionalDetails;
    }

    public void setAdditionalDetails(String additionalDetails) {
        this.additionalDetails = additionalDetails;
    }

    public boolean getHasAdditionalDetails() {
        return hasAdditionalDetails;
    }

    public void setHasAdditionalDetails(boolean hasAdditionalDetails) {
        this.hasAdditionalDetails = hasAdditionalDetails;
    }

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    public Set<ExtensionProvidedServiceApiEntity> getProvidedServiceApis() {
        return providedServiceApis;
    }

    public void setProvidedServiceApis(Set<ExtensionProvidedServiceApiEntity> providedServiceApis) {
        this.providedServiceApis = providedServiceApis;
    }

    public Set<ExtensionRestrictionEntity> getRestrictions() {
        return restrictions;
    }

    public void setRestrictions(Set<ExtensionRestrictionEntity> restrictions) {
        this.restrictions = restrictions;
    }


    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(String bucketId) {
        this.bucketId = bucketId;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getBundleId() {
        return bundleId;
    }

    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public void setArtifactId(String artifactId) {
        this.artifactId = artifactId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getSystemApiVersion() {
        return systemApiVersion;
    }

    public void setSystemApiVersion(String systemApiVersion) {
        this.systemApiVersion = systemApiVersion;
    }

    public BundleType getBundleType() {
        return bundleType;
    }

    public void setBundleType(BundleType bundleType) {
        this.bundleType = bundleType;
    }
}
