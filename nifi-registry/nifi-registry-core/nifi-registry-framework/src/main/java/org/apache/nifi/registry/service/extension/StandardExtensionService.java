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
package org.apache.nifi.registry.service.extension;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.bundle.extract.BundleExtractor;
import org.apache.nifi.registry.bundle.model.BundleDetails;
import org.apache.nifi.registry.bundle.model.BundleIdentifier;
import org.apache.nifi.registry.db.entity.BucketEntity;
import org.apache.nifi.registry.db.entity.BundleEntity;
import org.apache.nifi.registry.db.entity.BundleVersionDependencyEntity;
import org.apache.nifi.registry.db.entity.BundleVersionEntity;
import org.apache.nifi.registry.db.entity.ExtensionAdditionalDetailsEntity;
import org.apache.nifi.registry.db.entity.ExtensionEntity;
import org.apache.nifi.registry.exception.ResourceNotFoundException;
import org.apache.nifi.registry.extension.BundleCoordinate;
import org.apache.nifi.registry.extension.BundlePersistenceContext;
import org.apache.nifi.registry.extension.BundlePersistenceProvider;
import org.apache.nifi.registry.extension.BundleVersionCoordinate;
import org.apache.nifi.registry.extension.BundleVersionType;
import org.apache.nifi.registry.extension.bundle.BuildInfo;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.registry.extension.bundle.BundleVersionDependency;
import org.apache.nifi.registry.extension.bundle.BundleVersionFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.apache.nifi.registry.extension.component.ExtensionFilterParams;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.extension.component.TagCount;
import org.apache.nifi.registry.extension.component.manifest.Extension;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;
import org.apache.nifi.registry.extension.repo.ExtensionRepoArtifact;
import org.apache.nifi.registry.extension.repo.ExtensionRepoBucket;
import org.apache.nifi.registry.extension.repo.ExtensionRepoGroup;
import org.apache.nifi.registry.extension.repo.ExtensionRepoVersionSummary;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.provider.extension.StandardBundleCoordinate;
import org.apache.nifi.registry.provider.extension.StandardBundlePersistenceContext;
import org.apache.nifi.registry.provider.extension.StandardBundleVersionCoordinate;
import org.apache.nifi.registry.security.authorization.user.NiFiUserUtils;
import org.apache.nifi.registry.serialization.Serializer;
import org.apache.nifi.registry.service.MetadataService;
import org.apache.nifi.registry.service.extension.docs.DocumentationConstants;
import org.apache.nifi.registry.service.extension.docs.ExtensionDocWriter;
import org.apache.nifi.registry.service.mapper.BucketMappings;
import org.apache.nifi.registry.service.mapper.ExtensionMappings;
import org.apache.nifi.registry.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class StandardExtensionService implements ExtensionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardExtensionService.class);

    static final String SNAPSHOT_VERSION_SUFFIX = "SNAPSHOT";

    private final Serializer<Extension> extensionSerializer;
    private final ExtensionDocWriter extensionDocWriter;
    private final MetadataService metadataService;
    private final Map<BundleType, BundleExtractor> extractors;
    private final BundlePersistenceProvider bundlePersistenceProvider;
    private final Validator validator;
    private final File extensionsWorkingDir;

    @Autowired
    public StandardExtensionService(final Serializer<Extension> extensionSerializer,
                                    final ExtensionDocWriter extensionDocWriter,
                                    final MetadataService metadataService,
                                    final Map<BundleType, BundleExtractor> extractors,
                                    final BundlePersistenceProvider bundlePersistenceProvider,
                                    final Validator validator,
                                    final NiFiRegistryProperties properties) {
        this.extensionSerializer = extensionSerializer;
        this.extensionDocWriter = extensionDocWriter;
        this.metadataService = metadataService;
        this.extractors = extractors;
        this.bundlePersistenceProvider = bundlePersistenceProvider;
        this.validator = validator;
        this.extensionsWorkingDir = properties.getExtensionsWorkingDirectory();
        Validate.notNull(this.extensionSerializer);
        Validate.notNull(this.metadataService);
        Validate.notNull(this.extractors);
        Validate.notNull(this.bundlePersistenceProvider);
        Validate.notNull(this.validator);
        Validate.notNull(this.extensionsWorkingDir);
    }

    private <T>  void validate(T t, String invalidMessage) {
        final Set<ConstraintViolation<T>> violations = validator.validate(t);
        if (violations.size() > 0) {
            throw new ConstraintViolationException(invalidMessage, violations);
        }
    }

    @Override
    public BundleVersion createBundleVersion(final String bucketIdentifier, final BundleType bundleType,
                                             final InputStream inputStream, final String clientSha256) throws IOException {
        if (StringUtils.isBlank(bucketIdentifier)) {
            throw new IllegalArgumentException("Bucket identifier cannot be null or blank");
        }

        if (bundleType == null) {
            throw new IllegalArgumentException("Bundle type cannot be null");
        }

        if (inputStream == null) {
            throw new IllegalArgumentException("Extension bundle input stream cannot be null");
        }

        if (!extractors.containsKey(bundleType)) {
            throw new IllegalArgumentException("No metadata extractor is registered for bundle-type: " + bundleType);
        }

        // ensure the bucket exists
        final BucketEntity existingBucket = getBucketEntity(bucketIdentifier);

        // ensure the extensions directory exists and we can read and write to it
        FileUtils.ensureDirectoryExistAndCanReadAndWrite(extensionsWorkingDir);

        final String extensionWorkingFilename = UUID.randomUUID().toString();
        final File extensionWorkingFile = new File(extensionsWorkingDir, extensionWorkingFilename);
        LOGGER.debug("Writing bundle contents to working directory at {}", new Object[]{extensionWorkingFile.getAbsolutePath()});

        try {
            // write the contents of the input stream to a temporary file in the extensions working directory
            final MessageDigest sha256Digest = DigestUtils.getSha256Digest();
            try (final DigestInputStream digestInputStream = new DigestInputStream(inputStream, sha256Digest);
                 final OutputStream out = new FileOutputStream(extensionWorkingFile)) {
                IOUtils.copy(digestInputStream, out);
            }

            // get the hex of the SHA-256 computed by the server and compare to the client provided SHA-256, if one was provided
            final String sha256Hex = Hex.encodeHexString(sha256Digest.digest());
            final boolean sha256Supplied = !StringUtils.isBlank(clientSha256);
            if (sha256Supplied && !sha256Hex.equalsIgnoreCase(clientSha256)) {
                LOGGER.error("Client provided SHA-256 of '{}', but server calculated '{}'", new Object[]{clientSha256, sha256Hex});
                throw new IllegalStateException("The SHA-256 of the received extension bundle does not match the SHA-256 provided by the client");
            }

            // extract the details of the bundle from the temp file in the working directory
            final BundleDetails bundleDetails;
            try (final InputStream in = new FileInputStream(extensionWorkingFile)) {
                final BundleExtractor extractor = extractors.get(bundleType);
                bundleDetails = extractor.extract(in);
            }

            final BundleIdentifier bundleIdentifier = bundleDetails.getBundleIdentifier();
            final BuildInfo buildInfo = bundleDetails.getBuildInfo();

            final String groupId = bundleIdentifier.getGroupId();
            final String artifactId = bundleIdentifier.getArtifactId();
            final String version = bundleIdentifier.getVersion();

            final boolean isSnapshotVersion = version.endsWith(SNAPSHOT_VERSION_SUFFIX);
            final boolean overwriteBundleVersion = isSnapshotVersion || existingBucket.isAllowExtensionBundleRedeploy();

            LOGGER.debug("Extracted bundle details - '{}:{}:{}'", new Object[]{groupId, artifactId, version});

            // a bundle with the same group, artifact, and version can exist in multiple buckets, but only if it contains the same binary content, or if its a snapshot version
            // we can determine that by comparing the SHA-256 digest of the incoming bundle against existing bundles with the same group, artifact, version
            final List<BundleVersionEntity> allExistingVersions = metadataService.getBundleVersionsGlobal(groupId, artifactId, version);
            for (final BundleVersionEntity existingVersionEntity : allExistingVersions) {
                if (!existingVersionEntity.getSha256Hex().equals(sha256Hex) && !isSnapshotVersion) {
                    throw new IllegalStateException("Found existing extension bundle with same group, artifact, and version, but different SHA-256 checksums");
                }
            }

            // get the existing extension bundle entity, or create a new one if one does not exist in the bucket with the group + artifact
            final long currentTime = System.currentTimeMillis();
            final BundleEntity bundleEntity = getOrCreateExtensionBundle(bucketIdentifier, groupId, artifactId, bundleType, currentTime);

            // check if the version of incoming bundle already exists in the bucket
            // if it exists and it is a snapshot version or the bucket allows redeploying, then first delete the row in the extension_bundle_version table so we can create a new one
            // otherwise we throw an exception because we don't allow the same version in the same bucket
            final BundleVersionEntity existingVersion = metadataService.getBundleVersion(bucketIdentifier, groupId, artifactId, version);
            if (existingVersion != null) {
                if (overwriteBundleVersion) {
                    LOGGER.debug("Bundle overwriting allowed, deleting existing version...");
                    metadataService.deleteBundleVersion(existingVersion);
                } else {
                    LOGGER.warn("The specified version [{}] already exists for extension bundle [{}].", new Object[]{version, bundleEntity.getId()});
                    throw new IllegalStateException("The specified version already exists for the given extension bundle");
                }
            }

            // create the version metadata instance and validate it has all the required fields
            final String userIdentity = NiFiUserUtils.getNiFiUserIdentity();
            final BundleVersionMetadata versionMetadata = new BundleVersionMetadata();
            versionMetadata.setId(UUID.randomUUID().toString());
            versionMetadata.setBundleId(bundleEntity.getId());
            versionMetadata.setBucketId(bucketIdentifier);
            versionMetadata.setVersion(version);
            versionMetadata.setTimestamp(currentTime);
            versionMetadata.setAuthor(userIdentity);
            versionMetadata.setSha256(sha256Hex);
            versionMetadata.setSha256Supplied(sha256Supplied);
            versionMetadata.setContentSize(extensionWorkingFile.length());
            versionMetadata.setSystemApiVersion(bundleDetails.getSystemApiVersion());
            versionMetadata.setBuildInfo(buildInfo);

            validate(versionMetadata, "Cannot create extension bundle version");

            // create the bundle version in the metadata db
            final BundleVersionEntity versionEntity = ExtensionMappings.map(versionMetadata);
            metadataService.createBundleVersion(versionEntity);

            // create and persist the version dependencies in the metadata db
            final Set<BundleVersionDependencyEntity> dependencyEntities = getDependencyEntities(versionEntity, bundleDetails);
            dependencyEntities.forEach(d -> metadataService.createDependency(d));

            // create and persist extensions in the metadata db
            final Set<ExtensionEntity> extensionEntities = getExtensionEntities(versionEntity, bundleDetails);
            extensionEntities.forEach(e -> metadataService.createExtension(e));

            // persist the content of the bundle to the persistence provider
            persistBundleVersionContent(bundleType, bundleEntity, versionEntity, extensionWorkingFile, overwriteBundleVersion);

            // get the updated extension bundle so it contains the correct version count
            final BundleEntity updatedBundle = metadataService.getBundle(bucketIdentifier, groupId, artifactId);

            // create the full BundleVersion instance to return
            final BundleVersion bundleVersion = new BundleVersion();
            bundleVersion.setVersionMetadata(versionMetadata);
            bundleVersion.setBundle(ExtensionMappings.map(existingBucket, updatedBundle));
            bundleVersion.setBucket(BucketMappings.map(existingBucket));

            final Set<BundleVersionDependency> dependencies = new HashSet<>();
            dependencyEntities.forEach(d -> dependencies.add(ExtensionMappings.map(d)));
            bundleVersion.setDependencies(dependencies);

            LOGGER.debug("Created bundle - '{}:{}:{}'", new Object[]{groupId, artifactId, version});
            return bundleVersion;

        } finally {
            if (extensionWorkingFile.exists()) {
                try {
                    extensionWorkingFile.delete();
                } catch (Exception e) {
                    LOGGER.warn("Error removing temporary extension bundle file at {}",
                            new Object[]{extensionWorkingFile.getAbsolutePath()});
                }
            }
        }
    }

    private Set<BundleVersionDependencyEntity> getDependencyEntities(final BundleVersionEntity versionEntity, final BundleDetails bundleDetails) {
        final Set<BundleIdentifier> dependencyCoordinates = bundleDetails.getDependencies();
        if (dependencyCoordinates == null) {
            return Collections.emptySet();
        }

        final Set<BundleVersionDependencyEntity> versionDependencies = new HashSet<>();

        for (final BundleIdentifier dependencyCoordinate : dependencyCoordinates) {
            final BundleVersionDependency versionDependency = new BundleVersionDependency();
            versionDependency.setGroupId(dependencyCoordinate.getGroupId());
            versionDependency.setArtifactId(dependencyCoordinate.getArtifactId());
            versionDependency.setVersion(dependencyCoordinate.getVersion());
            validate(versionDependency, "Cannot create extension bundle version dependency");

            final BundleVersionDependencyEntity versionDependencyEntity = ExtensionMappings.map(versionDependency);
            versionDependencyEntity.setId(UUID.randomUUID().toString());
            versionDependencyEntity.setExtensionBundleVersionId(versionEntity.getId());

            versionDependencies.add(versionDependencyEntity);
        }

        return versionDependencies;
    }

    private Set<ExtensionEntity> getExtensionEntities(final BundleVersionEntity versionEntity, final BundleDetails bundleDetails) {
        final Set<Extension> extensions = bundleDetails.getExtensions();
        if (extensions == null) {
            return Collections.emptySet();
        }

        final Set<ExtensionEntity> extensionEntities = new HashSet<>();
        final Map<String,String> additionalDetails = bundleDetails.getAdditionalDetails();

        for (final Extension extension : extensions) {
            validate(extension, "Invalid extension due to one or more constraint violations");

            // Convert Extension to ExtensionEntity and populate ids
            final ExtensionEntity extensionEntity = ExtensionMappings.map(extension, extensionSerializer);
            extensionEntity.setId(UUID.randomUUID().toString());
            extensionEntity.setBundleVersionId(versionEntity.getId());

            extensionEntity.getRestrictions().forEach(r -> {
                r.setId(UUID.randomUUID().toString());
                r.setExtensionId(extensionEntity.getId());
            });

            extensionEntity.getProvidedServiceApis().forEach(p -> {
                p.setId(UUID.randomUUID().toString());
                p.setExtensionId(extensionEntity.getId());
            });

            // Check the additionalDetails map to see if there is an entry, and if so populate it
            final String additionalDetailsContent = additionalDetails.get(extensionEntity.getName());
            if (!StringUtils.isBlank(additionalDetailsContent)) {
                LOGGER.debug("Found additional details documentation for extension '{}'", new Object[]{extensionEntity.getName()});
                extensionEntity.setAdditionalDetails(additionalDetailsContent);
            }

            extensionEntities.add(extensionEntity);
        }

        return extensionEntities;
    }


    private BundleEntity getOrCreateExtensionBundle(final String bucketId, final String groupId, final String artifactId,
                                                    final BundleType bundleType, final long currentTime) {

        BundleEntity existingBundleEntity = metadataService.getBundle(bucketId, groupId, artifactId);
        if (existingBundleEntity == null) {
            final Bundle bundle = new Bundle();
            bundle.setIdentifier(UUID.randomUUID().toString());
            bundle.setBucketIdentifier(bucketId);
            bundle.setName(groupId + ":" + artifactId);
            bundle.setGroupId(groupId);
            bundle.setArtifactId(artifactId);
            bundle.setBundleType(bundleType);
            bundle.setCreatedTimestamp(currentTime);
            bundle.setModifiedTimestamp(currentTime);

            validate(bundle, "Cannot create extension bundle");
            existingBundleEntity = metadataService.createBundle(ExtensionMappings.map(bundle));
        } else {
            if (bundleType != existingBundleEntity.getBundleType()) {
                throw new IllegalStateException("A bundle already exists with the same group id and artifact id, but a different bundle type");
            }
        }

        return existingBundleEntity;
    }

    private void persistBundleVersionContent(final BundleType bundleType, final BundleEntity bundle, final BundleVersionEntity bundleVersion,
                                             final File extensionWorkingFile, final boolean overwriteBundleVersion) throws IOException {

        final BundleVersionCoordinate versionCoordinate = new StandardBundleVersionCoordinate.Builder()
                .bucketId(bundle.getBucketId())
                .groupId(bundle.getGroupId())
                .artifactId(bundle.getArtifactId())
                .version(bundleVersion.getVersion())
                .type(getProviderBundleType(bundleType))
                .build();

        final BundlePersistenceContext context = new StandardBundlePersistenceContext.Builder()
                .coordinate(versionCoordinate)
                .bundleSize(bundleVersion.getContentSize())
                .author(bundleVersion.getCreatedBy())
                .timestamp(bundleVersion.getCreated().getTime())
                .build();

        try (final InputStream in = new FileInputStream(extensionWorkingFile);
             final InputStream bufIn = new BufferedInputStream(in)) {
            if (overwriteBundleVersion) {
                bundlePersistenceProvider.updateBundleVersion(context, bufIn);
                LOGGER.debug("Bundle version updated in persistence provider - {}", new Object[]{versionCoordinate.toString()});
            } else {
                bundlePersistenceProvider.createBundleVersion(context, bufIn);
                LOGGER.debug("Bundle version created in persistence provider - {}", new Object[]{versionCoordinate.toString()});
            }
        }
    }

    private BundleVersionType getProviderBundleType(final BundleType bundleType) {
        switch (bundleType) {
            case NIFI_NAR:
                return BundleVersionType.NIFI_NAR;
            case MINIFI_CPP:
                return BundleVersionType.MINIFI_CPP;
            default:
                throw new IllegalArgumentException("Unknown bundle type: " + bundleType.toString());
        }
    }

    @Override
    public List<Bundle> getBundles(final Set<String> bucketIdentifiers, final BundleFilterParams filterParams) {
        if (bucketIdentifiers == null) {
            throw new IllegalArgumentException("Bucket identifiers cannot be null");
        }

        final List<BundleEntity> bundleEntities = metadataService.getBundles(bucketIdentifiers,
                filterParams == null ? BundleFilterParams.empty() : filterParams);
        return bundleEntities.stream().map(b -> ExtensionMappings.map(null, b)).collect(Collectors.toList());
    }

    @Override
    public List<Bundle> getBundlesByBucket(final String bucketIdentifier) {
        if (StringUtils.isBlank(bucketIdentifier)) {
            throw new IllegalArgumentException("Bucket identifier cannot be null or blank");
        }
        final BucketEntity existingBucket = getBucketEntity(bucketIdentifier);

        final List<BundleEntity> bundleEntities = metadataService.getBundlesByBucket(bucketIdentifier);
        return bundleEntities.stream().map(b -> ExtensionMappings.map(existingBucket, b)).collect(Collectors.toList());
    }

    @Override
    public Bundle getBundle(final String bundleIdentifier) {
        if (StringUtils.isBlank(bundleIdentifier)) {
            throw new IllegalArgumentException("Bundle identifier cannot be null or blank");
        }

        final BundleEntity existingBundle = getBundleEntity(bundleIdentifier);
        final BucketEntity existingBucket = getBucketEntity(existingBundle.getBucketId());
        return ExtensionMappings.map(existingBucket, existingBundle);
    }

    @Override
    public Bundle deleteBundle(final Bundle bundle) {
        if (bundle == null) {
            throw new IllegalArgumentException("Extension bundle cannot be null");
        }

        // delete the bundle from the database
        metadataService.deleteBundle(bundle.getIdentifier());

        // delete all content associated with the bundle in the persistence provider
        final BundleCoordinate bundleCoordinate = new StandardBundleCoordinate.Builder()
                .bucketId(bundle.getBucketIdentifier())
                .groupId(bundle.getGroupId())
                .artifactId(bundle.getArtifactId())
                .build();

        bundlePersistenceProvider.deleteAllBundleVersions(bundleCoordinate);

        return bundle;
    }

    // ---- BundleVersion methods -----

    @Override
    public SortedSet<BundleVersionMetadata> getBundleVersions(final Set<String> bucketIdentifiers,
                                                              final BundleVersionFilterParams filterParams) {
        if (bucketIdentifiers == null) {
            throw new IllegalArgumentException("Bucket identifiers cannot be null");
        }

        final SortedSet<BundleVersionMetadata> sortedVersions = new TreeSet<>(
                Comparator.comparing(BundleVersionMetadata::getBundleId)
                        .thenComparing(BundleVersionMetadata::getVersion)
        );

        final List<BundleVersionEntity> bundleVersionEntities = metadataService.getBundleVersions(bucketIdentifiers,
                filterParams == null ? BundleVersionFilterParams.empty() : filterParams);
        if (bundleVersionEntities != null) {
            bundleVersionEntities.forEach(bv -> sortedVersions.add(ExtensionMappings.map(bv)));
        }
        return sortedVersions;
    }

    @Override
    public SortedSet<BundleVersionMetadata> getBundleVersions(final String bundleIdentifier) {
        if (StringUtils.isBlank(bundleIdentifier)) {
            throw new IllegalArgumentException("Extension bundle identifier cannot be null or blank");
        }

        // ensure the bundle exists
        final BundleEntity existingBundle = getBundleEntity(bundleIdentifier);

        return getExtensionBundleVersionsSet(existingBundle);
    }

    private SortedSet<BundleVersionMetadata> getExtensionBundleVersionsSet(final BundleEntity existingBundle) {
        final SortedSet<BundleVersionMetadata> sortedVersions = new TreeSet<>(Collections.reverseOrder());

        final List<BundleVersionEntity> existingVersions = metadataService.getBundleVersions(existingBundle.getId());
        if (existingVersions != null) {
            existingVersions.stream().forEach(s -> sortedVersions.add(ExtensionMappings.map(s)));
        }
        return sortedVersions;
    }

    @Override
    public BundleVersion getBundleVersion(final String bucketId, final String bundleId, final String version) {
        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket id cannot be null or blank");
        }

        if (StringUtils.isBlank(bundleId)) {
            throw new IllegalArgumentException("Bundle id cannot be null or blank");
        }

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version cannot be null or blank");
        }

        // ensure the bucket exists
        final BucketEntity existingBucket = getBucketEntity(bucketId);

        // ensure the bundle exists
        final BundleEntity existingBundle = getBundleEntity(bundleId);

        if (!existingBucket.getId().equals(existingBundle.getBucketId())) {
            throw new IllegalStateException("The requested bundle is not located in the given bucket");
        }

        // retrieve the version of the bundle...
        final BundleVersionEntity existingVersion = metadataService.getBundleVersion(bundleId, version);
        if (existingVersion == null) {
            LOGGER.warn("The specified version [{}] does not exist for extension bundle [{}].", new Object[]{version, bundleId});
            throw new ResourceNotFoundException("The specified extension bundle version does not exist.");
        }
        return getBundleVersion(existingBucket, existingBundle, existingVersion);

    }

    @Override
    public BundleVersion getBundleVersion(final String bucketId, final String groupId, final String artifactId, final String version) {
        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket id cannot be null or blank");
        }

        if (StringUtils.isBlank(groupId)) {
            throw new IllegalArgumentException("Group id cannot be null or blank");
        }

        if (StringUtils.isBlank(artifactId)) {
            throw new IllegalArgumentException("Artifact id cannot be null or blank");
        }

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version cannot be null or blank");
        }

        // ensure the bucket exists
        final BucketEntity existingBucket = getBucketEntity(bucketId);

        // ensure the bundle exists
        final BundleEntity existingBundle = metadataService.getBundle(bucketId, groupId, artifactId);
        if (existingBundle == null) {
            LOGGER.warn("The specified extension bundle [{}-{}-{}] does not exist.", new Object[]{bucketId, groupId, artifactId});
            throw new ResourceNotFoundException("The specified extension bundle does not exist in this bucket.");
        }

        //ensure the version of the bundle exists
        final BundleVersionEntity existingVersion = metadataService.getBundleVersion(bucketId, groupId, artifactId, version);
        if (existingVersion == null) {
            LOGGER.warn("The specified extension bundle version [{}-{}-{}-{}] does not exist.", new Object[]{bucketId, groupId, artifactId, version});
            throw new ResourceNotFoundException("The specified extension bundle version does not exist in this bucket.");
        }

        // get the dependencies for the bundle version
        return getBundleVersion(existingBucket, existingBundle, existingVersion);
    }

    private BundleVersion getBundleVersion(final BucketEntity existingBucket, final BundleEntity existingBundle, final BundleVersionEntity existingVersion) {
        // get the dependencies for the bundle version
        final List<BundleVersionDependencyEntity> existingVersionDependencies = metadataService
                .getDependenciesForBundleVersion(existingVersion.getId());

        // convert the dependency db entities
        final Set<BundleVersionDependency> dependencies = existingVersionDependencies.stream()
                .map(d -> ExtensionMappings.map(d))
                .collect(Collectors.toSet());

        // create the full BundleVersion instance to return
        final BundleVersion bundleVersion = new BundleVersion();
        bundleVersion.setVersionMetadata(ExtensionMappings.map(existingVersion));
        bundleVersion.setBundle(ExtensionMappings.map(existingBucket, existingBundle));
        bundleVersion.setBucket(BucketMappings.map(existingBucket));
        bundleVersion.setDependencies(dependencies);
        return bundleVersion;
    }

    @Override
    public void writeBundleVersionContent(final BundleVersion bundleVersion, final OutputStream out) {
        // get the content from the persistence provider and write it to the output stream
        final BundleVersionCoordinate versionCoordinate = getVersionCoordinate(bundleVersion);
        bundlePersistenceProvider.getBundleVersionContent(versionCoordinate, out);
    }

    @Override
    public BundleVersion deleteBundleVersion(final BundleVersion bundleVersion) {
        if (bundleVersion == null) {
            throw new IllegalArgumentException("Extension bundle version cannot be null");
        }

        // delete from the metadata db
        final String extensionBundleVersionId = bundleVersion.getVersionMetadata().getId();
        metadataService.deleteBundleVersion(extensionBundleVersionId);

        // delete content associated with the bundle version in the persistence provider
        final BundleVersionCoordinate versionCoordinate = getVersionCoordinate(bundleVersion);
        bundlePersistenceProvider.deleteBundleVersion(versionCoordinate);

        return bundleVersion;
    }

    // ------ Extension Methods ----

    @Override
    public SortedSet<ExtensionMetadata> getExtensionMetadata(final Set<String> bucketIdentifiers, final ExtensionFilterParams filterParams) {
        if (bucketIdentifiers == null) {
            throw new IllegalArgumentException("Bucket identifiers cannot be null");
        }

        // retrieve the extension entities
        final List<ExtensionEntity> extensionEntities = metadataService.getExtensions(bucketIdentifiers, filterParams);
        return getExtensionMetadata(extensionEntities);
    }

    @Override
    public SortedSet<ExtensionMetadata> getExtensionMetadata(final Set<String> bucketIdentifiers, final ProvidedServiceAPI serviceAPI) {
        if (bucketIdentifiers == null) {
            throw new IllegalArgumentException("Bucket identifiers cannot be null");
        }

        if (serviceAPI == null
                || StringUtils.isBlank(serviceAPI.getClassName())
                || StringUtils.isBlank(serviceAPI.getGroupId())
                || StringUtils.isBlank(serviceAPI.getArtifactId())
                || StringUtils.isBlank(serviceAPI.getVersion())) {
            throw new IllegalArgumentException("Provided service API must be specified with a class, group, artifact, and version");
        }

        // retrieve the extension entities
        final List<ExtensionEntity> extensionEntities = metadataService.getExtensionsByProvidedServiceApi(bucketIdentifiers, serviceAPI);
        return getExtensionMetadata(extensionEntities);
    }

    private SortedSet<ExtensionMetadata> getExtensionMetadata(List<ExtensionEntity> extensionEntities) {
        // map to extension metadata and sort by extension name
        final SortedSet<ExtensionMetadata> extensions = new TreeSet<>();
        extensionEntities.forEach(e -> {
            final ExtensionMetadata metadata = ExtensionMappings.mapToMetadata(e, extensionSerializer);
            extensions.add(metadata);
        });
        return extensions;
    }

    @Override
    public SortedSet<ExtensionMetadata> getExtensionMetadata(final BundleVersion bundleVersion) {
        if (bundleVersion == null) {
            throw new IllegalArgumentException("Extension bundle version cannot be null");
        }

        // ensure the bundle version exists
        final BundleVersionEntity existingBundleVersion = metadataService.getBundleVersion(
                bundleVersion.getVersionMetadata().getBucketId(),
                bundleVersion.getBundle().getGroupId(),
                bundleVersion.getBundle().getArtifactId(),
                bundleVersion.getVersionMetadata().getVersion());

        if (existingBundleVersion == null) {
            LOGGER.warn("The specified extension bundle version does not exist for [{}] - [{}] - [{}] - [{}]",
                    new Object[]{
                            bundleVersion.getVersionMetadata().getBucketId(),
                            bundleVersion.getBundle().getGroupId(),
                            bundleVersion.getBundle().getArtifactId(),
                            bundleVersion.getVersionMetadata().getVersion()});
            throw new ResourceNotFoundException("The specified extension bundle version does not exist.");
        }

        // retrieve the extension entities
        final List<ExtensionEntity> extensionEntities = metadataService.getExtensionsByBundleVersionId(existingBundleVersion.getId());

        // map to extension and sort by extension name
        final SortedSet<ExtensionMetadata> extensions = new TreeSet<>();
        extensionEntities.forEach(e -> extensions.add(ExtensionMappings.mapToMetadata(e, extensionSerializer)));
        return extensions;
    }

    @Override
    public Extension getExtension(final BundleVersion bundleVersion, final String name) {
        if (bundleVersion == null) {
            throw new IllegalArgumentException("Bundle version cannot be null");
        }

        if (bundleVersion.getVersionMetadata() == null || StringUtils.isBlank(bundleVersion.getVersionMetadata().getId())) {
            throw new IllegalArgumentException("Bundle version must contain a version metadata with a bundle version id");
        }

        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Extension name cannot be null or blank");
        }

        final ExtensionEntity entity = metadataService.getExtensionByName(bundleVersion.getVersionMetadata().getId(), name);
        if (entity == null) {
            LOGGER.warn("The specified extension [{}] does not exist in the specified bundle version [{}].",
                    new Object[]{name, bundleVersion.getVersionMetadata().getId()});
            throw new ResourceNotFoundException("The specified extension does not exist in this registry.");
        }

        return ExtensionMappings.map(entity, extensionSerializer);
    }

    @Override
    public void writeExtensionDocs(final BundleVersion bundleVersion, final String name, final OutputStream outputStream)
            throws IOException {
        if (bundleVersion == null) {
            throw new IllegalArgumentException("Bundle version cannot be null");
        }

        if (bundleVersion.getVersionMetadata() == null || StringUtils.isBlank(bundleVersion.getVersionMetadata().getId())) {
            throw new IllegalArgumentException("Bundle version must contain a version metadata with a bundle version id");
        }

        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Extension name cannot be null or blank");
        }

        if (outputStream == null) {
            throw new IllegalArgumentException("Output stream cannot be null");
        }

        final ExtensionEntity entity = metadataService.getExtensionByName(bundleVersion.getVersionMetadata().getId(), name);
        if (entity == null) {
            LOGGER.warn("The specified extension [{}] does not exist in the specified bundle version [{}].",
                    new Object[]{name, bundleVersion.getVersionMetadata().getId()});
            throw new ResourceNotFoundException("The specified extension does not exist in this registry.");
        }

        final ExtensionMetadata extensionMetadata = ExtensionMappings.mapToMetadata(entity, extensionSerializer);
        final Extension extension = ExtensionMappings.map(entity, extensionSerializer);
        extensionDocWriter.write(extensionMetadata, extension, outputStream);
    }

    @Override
    public void writeAdditionalDetailsDocs(final BundleVersion bundleVersion, final String name, final OutputStream outputStream) throws IOException {
        if (bundleVersion == null) {
            throw new IllegalArgumentException("Bundle version cannot be null");
        }

        if (bundleVersion.getVersionMetadata() == null || StringUtils.isBlank(bundleVersion.getVersionMetadata().getId())) {
            throw new IllegalArgumentException("Bundle version must contain a version metadata with a bundle version id");
        }

        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Extension name cannot be null or blank");
        }

        if (outputStream == null) {
            throw new IllegalArgumentException("Output stream cannot be null");
        }

        final ExtensionAdditionalDetailsEntity additionalDetailsEntity = metadataService.getExtensionAdditionalDetails(
                bundleVersion.getVersionMetadata().getId(), name);

        if (additionalDetailsEntity == null) {
            LOGGER.warn("The specified extension [{}] does not exist in the specified bundle version [{}].",
                    new Object[]{name, bundleVersion.getVersionMetadata().getId()});
            throw new ResourceNotFoundException("The specified extension does not exist in this registry.");
        }

        if (!additionalDetailsEntity.getAdditionalDetails().isPresent()) {
            LOGGER.warn("The specified extension [{}] does not have additional details in the specified bundle version [{}].",
                    new Object[]{name, bundleVersion.getVersionMetadata().getId()});
            throw new IllegalStateException("The specified extension does not have additional details.");
        }

        final String additionalDetailsContent = additionalDetailsEntity.getAdditionalDetails().get();

        // The additional details content may have come from NiFi which has a different path to the css so we need to fix the location
        final String componentUsageCssRef = DocumentationConstants.CSS_PATH + "component-usage.css";
        final String updatedContent = additionalDetailsContent.replace("../../../../../css/component-usage.css", componentUsageCssRef);

        IOUtils.write(updatedContent, outputStream, StandardCharsets.UTF_8);
    }

    @Override
    public SortedSet<TagCount> getExtensionTags() {
        final SortedSet<TagCount> tagCounts = new TreeSet<>();
        metadataService.getAllExtensionTags().forEach(tc -> tagCounts.add(ExtensionMappings.map(tc)));
        return tagCounts;
    }

    // ------ Extension Repository Methods -------

    @Override
    public SortedSet<ExtensionRepoBucket> getExtensionRepoBuckets(final Set<String> bucketIds) {
        if (bucketIds == null) {
            throw new IllegalArgumentException("Bucket ids cannot be null");
        }

        if (bucketIds.isEmpty()) {
            return new TreeSet<>();
        }

        final SortedSet<ExtensionRepoBucket> repoBuckets = new TreeSet<>();

        final List<BucketEntity> buckets = metadataService.getBuckets(bucketIds);
        buckets.forEach(b -> {
            final ExtensionRepoBucket repoBucket = new ExtensionRepoBucket();
            repoBucket.setBucketName(b.getName());
            repoBuckets.add(repoBucket);
        });

        return repoBuckets;
    }

    @Override
    public SortedSet<ExtensionRepoGroup> getExtensionRepoGroups(final Bucket bucket) {
        if (bucket == null) {
            throw new IllegalArgumentException("Bucket cannot be null");
        }

        final SortedSet<ExtensionRepoGroup> repoGroups = new TreeSet<>();

        final List<BundleEntity> bundleEntities = metadataService.getBundlesByBucket(bucket.getIdentifier());
        bundleEntities.forEach(b -> {
            final ExtensionRepoGroup repoGroup = new ExtensionRepoGroup();
            repoGroup.setBucketName(bucket.getName());
            repoGroup.setGroupId(b.getGroupId());
            repoGroups.add(repoGroup);
        });

        return repoGroups;
    }

    @Override
    public SortedSet<ExtensionRepoArtifact> getExtensionRepoArtifacts(final Bucket bucket, final String groupId) {
        if (bucket == null) {
            throw new IllegalArgumentException("Bucket cannot be null");
        }

        if (StringUtils.isBlank(groupId)) {
            throw new IllegalArgumentException("Group id cannot be null or blank");
        }

        final SortedSet<ExtensionRepoArtifact> repoArtifacts = new TreeSet<>();

        final List<BundleEntity> bundleEntities = metadataService.getBundlesByBucketAndGroup(bucket.getIdentifier(), groupId);
        bundleEntities.forEach(b -> {
            final ExtensionRepoArtifact repoArtifact = new ExtensionRepoArtifact();
            repoArtifact.setBucketName(bucket.getName());
            repoArtifact.setGroupId(b.getGroupId());
            repoArtifact.setArtifactId(b.getArtifactId());
            repoArtifacts.add(repoArtifact);
        });

        return repoArtifacts;
    }

    @Override
    public SortedSet<ExtensionRepoVersionSummary> getExtensionRepoVersions(final Bucket bucket, final String groupId, final String artifactId) {
        if (bucket == null) {
            throw new IllegalArgumentException("Bucket cannot be null");
        }

        if (StringUtils.isBlank(groupId)) {
            throw new IllegalArgumentException("Group id cannot be null or blank");
        }

        if (StringUtils.isBlank(artifactId)) {
            throw new IllegalArgumentException("Artifact id cannot be null or blank");
        }

        final SortedSet<ExtensionRepoVersionSummary> repoVersions = new TreeSet<>();

        final List<BundleVersionEntity> versionEntities = metadataService.getBundleVersions(bucket.getIdentifier(), groupId, artifactId);
        if (!versionEntities.isEmpty()) {
            final BundleEntity bundleEntity = metadataService.getBundle(bucket.getIdentifier(), groupId, artifactId);
            if (bundleEntity == null) {
                // should never happen if the list of versions is not empty, but just in case
                throw new ResourceNotFoundException("The specified extension bundle does not exist in this bucket");
            }

            versionEntities.forEach(v -> {
                final ExtensionRepoVersionSummary repoVersion = new ExtensionRepoVersionSummary();
                repoVersion.setBucketName(bucket.getName());
                repoVersion.setGroupId(bundleEntity.getGroupId());
                repoVersion.setArtifactId(bundleEntity.getArtifactId());
                repoVersion.setVersion(v.getVersion());
                repoVersion.setAuthor(v.getCreatedBy());
                repoVersion.setTimestamp(v.getCreated().getTime());
                repoVersions.add(repoVersion);
            });
        }

        return repoVersions;
    }

    // ------ Helper Methods -------

    private BundleVersionCoordinate getVersionCoordinate(final BundleVersion bundleVersion) {
        return getVersionCoordinate(bundleVersion.getBundle(), bundleVersion.getVersionMetadata());
    }

    private BundleVersionCoordinate getVersionCoordinate(final Bundle bundle, final BundleVersionMetadata bundleVersionMetadata) {
        final BundleVersionCoordinate versionCoordinate = new StandardBundleVersionCoordinate.Builder()
                .bucketId(bundle.getBucketIdentifier())
                .groupId(bundle.getGroupId())
                .artifactId(bundle.getArtifactId())
                .version(bundleVersionMetadata.getVersion())
                .type(getProviderBundleType(bundle.getBundleType()))
                .build();

        return versionCoordinate;
    }

    private BucketEntity getBucketEntity(final String bucketIdentifier) {
        // ensure the bucket exists
        final BucketEntity existingBucket = metadataService.getBucketById(bucketIdentifier);
        if (existingBucket == null) {
            LOGGER.warn("The specified bucket id [{}] does not exist.", bucketIdentifier);
            throw new ResourceNotFoundException("The specified bucket ID does not exist in this registry.");
        }
        return existingBucket;
    }

    private BundleEntity getBundleEntity(final String bundleId) {
        final BundleEntity existingBundle = metadataService.getBundle(bundleId);
        if (existingBundle == null) {
            LOGGER.warn("The specified extension bundle id [{}] does not exist.", bundleId);
            throw new ResourceNotFoundException("The specified extension bundle ID does not exist.");
        }
        return existingBundle;
    }

}
