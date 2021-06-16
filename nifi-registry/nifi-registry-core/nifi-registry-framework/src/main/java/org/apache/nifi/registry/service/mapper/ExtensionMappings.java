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
package org.apache.nifi.registry.service.mapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.db.entity.BucketEntity;
import org.apache.nifi.registry.db.entity.BucketItemEntityType;
import org.apache.nifi.registry.db.entity.BundleEntity;
import org.apache.nifi.registry.db.entity.BundleVersionDependencyEntity;
import org.apache.nifi.registry.db.entity.BundleVersionEntity;
import org.apache.nifi.registry.db.entity.ExtensionEntity;
import org.apache.nifi.registry.db.entity.ExtensionProvidedServiceApiEntity;
import org.apache.nifi.registry.db.entity.ExtensionRestrictionEntity;
import org.apache.nifi.registry.db.entity.TagCountEntity;
import org.apache.nifi.registry.extension.bundle.BuildInfo;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleInfo;
import org.apache.nifi.registry.extension.bundle.BundleVersionDependency;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.extension.component.TagCount;
import org.apache.nifi.registry.extension.component.manifest.Extension;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;
import org.apache.nifi.registry.extension.component.manifest.Restriction;
import org.apache.nifi.registry.serialization.SerializationException;
import org.apache.nifi.registry.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.stream.Collectors;

/**
 * Mappings between Extension related DB entities and data model.
 */
public class ExtensionMappings {

    // -- Map Bundle

    public static BundleEntity map(final Bundle bundle) {
        final BundleEntity entity = new BundleEntity();
        entity.setId(bundle.getIdentifier());
        entity.setName(bundle.getName());
        entity.setDescription(bundle.getDescription());
        entity.setCreated(new Date(bundle.getCreatedTimestamp()));
        entity.setModified(new Date(bundle.getModifiedTimestamp()));
        entity.setType(BucketItemEntityType.BUNDLE);
        entity.setBucketId(bundle.getBucketIdentifier());

        entity.setGroupId(bundle.getGroupId());
        entity.setArtifactId(bundle.getArtifactId());
        entity.setBundleType(bundle.getBundleType());
        return entity;
    }

    public static Bundle map(final BucketEntity bucketEntity, final BundleEntity bundleEntity) {
        final Bundle bundle = new Bundle();
        bundle.setIdentifier(bundleEntity.getId());
        bundle.setName(bundleEntity.getName());
        bundle.setDescription(bundleEntity.getDescription());
        bundle.setCreatedTimestamp(bundleEntity.getCreated().getTime());
        bundle.setModifiedTimestamp(bundleEntity.getModified().getTime());
        bundle.setBucketIdentifier(bundleEntity.getBucketId());

        if (bucketEntity != null) {
            bundle.setBucketName(bucketEntity.getName());
        } else {
            bundle.setBucketName(bundleEntity.getBucketName());
        }

        bundle.setGroupId(bundleEntity.getGroupId());
        bundle.setArtifactId(bundleEntity.getArtifactId());
        bundle.setBundleType(bundleEntity.getBundleType());
        bundle.setVersionCount(bundleEntity.getVersionCount());
        return bundle;
    }

    // -- Map BundleVersion

    public static BundleVersionEntity map(final BundleVersionMetadata bundleVersionMetadata) {
        final BundleVersionEntity entity = new BundleVersionEntity();
        entity.setId(bundleVersionMetadata.getId());
        entity.setBundleId(bundleVersionMetadata.getBundleId());
        entity.setBucketId(bundleVersionMetadata.getBucketId());
        entity.setVersion(bundleVersionMetadata.getVersion());
        entity.setCreated(new Date(bundleVersionMetadata.getTimestamp()));
        entity.setCreatedBy(bundleVersionMetadata.getAuthor());
        entity.setDescription(bundleVersionMetadata.getDescription());
        entity.setSha256Hex(bundleVersionMetadata.getSha256());
        entity.setSha256Supplied(bundleVersionMetadata.getSha256Supplied());
        entity.setContentSize(bundleVersionMetadata.getContentSize());
        entity.setSystemApiVersion(bundleVersionMetadata.getSystemApiVersion());

        final BuildInfo buildInfo = bundleVersionMetadata.getBuildInfo();
        entity.setBuildTool(buildInfo.getBuildTool());
        entity.setBuildFlags(buildInfo.getBuildFlags());
        entity.setBuildBranch(buildInfo.getBuildBranch());
        entity.setBuildTag(buildInfo.getBuildTag());
        entity.setBuildRevision(buildInfo.getBuildRevision());
        entity.setBuiltBy(buildInfo.getBuiltBy());
        entity.setBuilt(new Date(buildInfo.getBuilt()));

        return entity;
    }

    public static BundleVersionMetadata map(final BundleVersionEntity bundleVersionEntity) {
        final BundleVersionMetadata bundleVersionMetadata = new BundleVersionMetadata();
        bundleVersionMetadata.setId(bundleVersionEntity.getId());
        bundleVersionMetadata.setBundleId(bundleVersionEntity.getBundleId());
        bundleVersionMetadata.setBucketId(bundleVersionEntity.getBucketId());
        bundleVersionMetadata.setGroupId(bundleVersionEntity.getGroupId());
        bundleVersionMetadata.setArtifactId(bundleVersionEntity.getArtifactId());
        bundleVersionMetadata.setVersion(bundleVersionEntity.getVersion());
        bundleVersionMetadata.setTimestamp(bundleVersionEntity.getCreated().getTime());
        bundleVersionMetadata.setAuthor(bundleVersionEntity.getCreatedBy());
        bundleVersionMetadata.setDescription(bundleVersionEntity.getDescription());
        bundleVersionMetadata.setSha256(bundleVersionEntity.getSha256Hex());
        bundleVersionMetadata.setSha256Supplied(bundleVersionEntity.getSha256Supplied());
        bundleVersionMetadata.setContentSize(bundleVersionEntity.getContentSize());
        bundleVersionMetadata.setSystemApiVersion(bundleVersionEntity.getSystemApiVersion());

        final BuildInfo buildInfo = new BuildInfo();
        buildInfo.setBuildTool(bundleVersionEntity.getBuildTool());
        buildInfo.setBuildFlags(bundleVersionEntity.getBuildFlags());
        buildInfo.setBuildBranch(bundleVersionEntity.getBuildBranch());
        buildInfo.setBuildTag(bundleVersionEntity.getBuildTag());
        buildInfo.setBuildRevision(bundleVersionEntity.getBuildRevision());
        buildInfo.setBuiltBy(bundleVersionEntity.getBuiltBy());
        buildInfo.setBuilt(bundleVersionEntity.getBuilt().getTime());
        bundleVersionMetadata.setBuildInfo(buildInfo);

        return bundleVersionMetadata;
    }

    // -- Map BundleVersionDependency

    public static BundleVersionDependencyEntity map(final BundleVersionDependency bundleVersionDependency) {
        final BundleVersionDependencyEntity entity = new BundleVersionDependencyEntity();
        entity.setGroupId(bundleVersionDependency.getGroupId());
        entity.setArtifactId(bundleVersionDependency.getArtifactId());
        entity.setVersion(bundleVersionDependency.getVersion());
        return entity;
    }

    public static BundleVersionDependency map(final BundleVersionDependencyEntity dependencyEntity) {
        final BundleVersionDependency dependency = new BundleVersionDependency();
        dependency.setGroupId(dependencyEntity.getGroupId());
        dependency.setArtifactId(dependencyEntity.getArtifactId());
        dependency.setVersion(dependencyEntity.getVersion());
        return dependency;
    }

    // -- Map Extension

    public static ExtensionEntity map(final Extension extension, final Serializer<Extension> extensionSerializer) {
        final String extensionContent;
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            extensionSerializer.serialize(extension, out);
            extensionContent = new String(out.toByteArray(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SerializationException("Unable to serialize extension", e);
        }

        final ExtensionEntity entity = new ExtensionEntity();
        entity.setName(extension.getName());

        // determine the display name which is the last part of the name after the last separator
        final String fullName = entity.getName();
        if (fullName != null) {
            final int index = fullName.lastIndexOf('.');
            if (index > 0 && (index < (fullName.length() - 1))) {
                entity.setDisplayName(fullName.substring(index + 1));
            }
        }

        // if displayName still isn't set, then set it to the full name
        if (StringUtils.isBlank(entity.getDisplayName())) {
            entity.setDisplayName(extension.getName());
        }

        entity.setExtensionType(extension.getType());
        entity.setContent(extensionContent);

        if (extension.getTags() != null) {
            entity.setTags(extension.getTags().stream().collect(Collectors.toSet()));
        }

        if (extension.getProvidedServiceAPIs() != null) {
            entity.setProvidedServiceApis(extension.getProvidedServiceAPIs().stream()
                    .map(p -> map(p))
                    .collect(Collectors.toSet()));
        } else {
            entity.setProvidedServiceApis(Collections.emptySet());
        }

        if (extension.getRestricted() != null) {
            if (extension.getRestricted().getRestrictions() != null) {
                entity.setRestrictions(extension.getRestricted().getRestrictions().stream()
                        .map(r -> map(r))
                        .collect(Collectors.toSet()));
            }
        } else {
            entity.setRestrictions(Collections.emptySet());
        }

        return entity;
    }

    public static Extension map(final ExtensionEntity entity, final Serializer<Extension> extensionSerializer) {
        final byte[] content = entity.getContent().getBytes(StandardCharsets.UTF_8);
        try (final ByteArrayInputStream input = new ByteArrayInputStream(content)) {
            return extensionSerializer.deserialize(input);
        } catch (IOException e) {
            throw new SerializationException("Unable to deserialize extension", e);
        }
    }

    // -- Map ExtensionMetadata

    public static ExtensionMetadata mapToMetadata(final ExtensionEntity entity, final Serializer<Extension> extensionSerializer) {
        final Extension extension = map(entity, extensionSerializer);

        final BundleInfo bundleInfo = new BundleInfo();
        bundleInfo.setBucketId(entity.getBucketId());
        bundleInfo.setBucketName(entity.getBucketName());
        bundleInfo.setBundleId(entity.getBundleId());
        bundleInfo.setGroupId(entity.getGroupId());
        bundleInfo.setArtifactId(entity.getArtifactId());
        bundleInfo.setVersion(entity.getVersion());
        bundleInfo.setBundleType(entity.getBundleType());
        bundleInfo.setSystemApiVersion(entity.getSystemApiVersion());

        final ExtensionMetadata metadata = new ExtensionMetadata();
        metadata.setName(extension.getName());
        metadata.setDisplayName(entity.getDisplayName());
        metadata.setType(extension.getType());
        metadata.setDescription(extension.getDescription());
        metadata.setDeprecationNotice(extension.getDeprecationNotice());
        metadata.setRestricted(extension.getRestricted());
        metadata.setProvidedServiceAPIs(extension.getProvidedServiceAPIs());
        metadata.setTags(extension.getTags());
        metadata.setBundleInfo(bundleInfo);
        metadata.setHasAdditionalDetails(entity.getHasAdditionalDetails());
        return metadata;
    }

    // -- Map ProvidedServiceAPI

    public static ExtensionProvidedServiceApiEntity map(final ProvidedServiceAPI providedServiceApi) {
        final ExtensionProvidedServiceApiEntity entity = new ExtensionProvidedServiceApiEntity();
        entity.setClassName(providedServiceApi.getClassName());
        entity.setGroupId(providedServiceApi.getGroupId());
        entity.setArtifactId(providedServiceApi.getArtifactId());
        entity.setVersion(providedServiceApi.getVersion());
        return entity;
    }

    public static ProvidedServiceAPI map(final ExtensionProvidedServiceApiEntity entity) {
        final ProvidedServiceAPI providedServiceApi = new ProvidedServiceAPI();
        providedServiceApi.setClassName(entity.getClassName());
        providedServiceApi.setGroupId(entity.getGroupId());
        providedServiceApi.setArtifactId(entity.getArtifactId());
        providedServiceApi.setVersion(entity.getVersion());
        return providedServiceApi;
    }

    // -- Map Restriction

    public static ExtensionRestrictionEntity map(final Restriction restriction) {
        final ExtensionRestrictionEntity restrictionEntity = new ExtensionRestrictionEntity();
        restrictionEntity.setRequiredPermission(restriction.getRequiredPermission());
        restrictionEntity.setExplanation(restriction.getExplanation());
        return restrictionEntity;
    }

    public static Restriction map(final ExtensionRestrictionEntity restrictionEntity) {
        final Restriction restriction = new Restriction();
        restriction.setRequiredPermission(restrictionEntity.getRequiredPermission());
        restriction.setExplanation(restrictionEntity.getExplanation());
        return restriction;
    }

    // -- Map TagCount

    public static TagCount map(final TagCountEntity entity) {
        final TagCount tagCount = new TagCount();
        tagCount.setTag(entity.getTag());
        tagCount.setCount(entity.getCount());
        return tagCount;
    }
}
