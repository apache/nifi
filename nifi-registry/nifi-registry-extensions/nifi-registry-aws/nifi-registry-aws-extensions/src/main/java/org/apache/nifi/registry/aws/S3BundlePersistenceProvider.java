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
package org.apache.nifi.registry.aws;

import org.apache.nifi.registry.extension.BundleCoordinate;
import org.apache.nifi.registry.extension.BundlePersistenceContext;
import org.apache.nifi.registry.extension.BundlePersistenceException;
import org.apache.nifi.registry.extension.BundlePersistenceProvider;
import org.apache.nifi.registry.extension.BundleVersionCoordinate;
import org.apache.nifi.registry.extension.BundleVersionType;
import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.apache.nifi.registry.provider.ProviderCreationException;
import org.apache.nifi.registry.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.utils.IoUtils;
import software.amazon.awssdk.utils.StringUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * An {@link BundlePersistenceProvider} that uses AWS S3 for storage.
 */
public class S3BundlePersistenceProvider implements BundlePersistenceProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3BundlePersistenceProvider.class);

    public static final String REGION_PROP = "Region";
    public static final String BUCKET_NAME_PROP = "Bucket Name";
    public static final String KEY_PREFIX_PROP = "Key Prefix";
    public static final String CREDENTIALS_PROVIDER_PROP = "Credentials Provider";
    public static final String ACCESS_KEY_PROP = "Access Key";
    public static final String SECRET_ACCESS_KEY_PROP = "Secret Access Key";
    public static final String ENDPOINT_URL_PROP = "Endpoint URL";

    public static final String NAR_EXTENSION = ".nar";
    public static final String CPP_EXTENSION = ".cpp";

    public enum CredentialProvider {
        STATIC,
        DEFAULT_CHAIN
    }

    private volatile S3Client s3Client;
    private volatile String s3BucketName;
    private volatile String s3KeyPrefix;

    @Override
    public void onConfigured(final ProviderConfigurationContext configurationContext) throws ProviderCreationException {
        s3BucketName = configurationContext.getProperties().get(BUCKET_NAME_PROP);
        if (StringUtils.isBlank(s3BucketName)) {
            throw new ProviderCreationException("The property '" + BUCKET_NAME_PROP + "' must be provided");
        }

        final String keyPrefixValue = configurationContext.getProperties().get(KEY_PREFIX_PROP);
        s3KeyPrefix = StringUtils.isBlank(keyPrefixValue) ? null : keyPrefixValue;

        s3Client = createS3Client(configurationContext);
    }

    protected S3Client createS3Client(final ProviderConfigurationContext configurationContext) {

        final S3ClientBuilder builder = S3Client.builder()
                .region(getRegion(configurationContext))
                .credentialsProvider(getCredentialsProvider(configurationContext));

        final URI s3EndpointOverride = getS3EndpointOverride(configurationContext);
        if (s3EndpointOverride != null) {
            builder.endpointOverride(s3EndpointOverride);
        }

        return builder.build();

    }

    private Region getRegion(final ProviderConfigurationContext configurationContext) {
        final String regionValue = configurationContext.getProperties().get(REGION_PROP);
        if (StringUtils.isBlank(regionValue)) {
            throw new ProviderCreationException("The property '" + REGION_PROP + "' must be provided");
        }

        Region region = null;
        for (Region r : Region.regions()) {
            if (r.id().equals(regionValue)) {
                region = r;
                break;
            }
        }

        if (region == null) {
            LOGGER.warn("The provided region was not found in the list of known regions. This may indicate an invalid region, " +
                    "or may indicate a region that is newer than the known list of regions");
            region = Region.of(regionValue);
        }

        LOGGER.debug("Using region {}", new Object[] {region.id()});
        return region;
    }

    private AwsCredentialsProvider getCredentialsProvider(final ProviderConfigurationContext configurationContext) {
        final String credentialsProviderValue = configurationContext.getProperties().get(CREDENTIALS_PROVIDER_PROP);
        if (StringUtils.isBlank(credentialsProviderValue)) {
            throw new ProviderCreationException("The property '" + CREDENTIALS_PROVIDER_PROP + "' must be provided");
        }

        CredentialProvider credentialProvider;
        try {
            credentialProvider = CredentialProvider.valueOf(credentialsProviderValue);
        } catch (Exception e) {
            throw new ProviderCreationException("The property '" + CREDENTIALS_PROVIDER_PROP + "' must be one of ["
                    + CredentialProvider.STATIC + ", " + CredentialProvider.DEFAULT_CHAIN + " ]");
        }

        if (CredentialProvider.STATIC == credentialProvider) {
            final String accesKeyValue = configurationContext.getProperties().get(ACCESS_KEY_PROP);
            final String secretAccessKey = configurationContext.getProperties().get(SECRET_ACCESS_KEY_PROP);

            if (StringUtils.isBlank(accesKeyValue) || StringUtils.isBlank(secretAccessKey)) {
                throw new ProviderCreationException("The properties '" + ACCESS_KEY_PROP + "' and '" + SECRET_ACCESS_KEY_PROP
                        + "' must be provided when using " + CredentialProvider.STATIC + " credentials provider");
            }

            LOGGER.debug("Creating StaticCredentialsProvider");
            final AwsCredentials awsCredentials = AwsBasicCredentials.create(accesKeyValue, secretAccessKey);
            return StaticCredentialsProvider.create(awsCredentials);

        } else {
            LOGGER.debug("Creating DefaultCredentialsProvider");
            return DefaultCredentialsProvider.create();
        }
    }

    private URI getS3EndpointOverride(final ProviderConfigurationContext configurationContext) {
        final URI s3EndpointOverride;
        final String endpointUrlValue = configurationContext.getProperties().get(ENDPOINT_URL_PROP);
        try {
            s3EndpointOverride = StringUtils.isBlank(endpointUrlValue) ? null : URI.create(endpointUrlValue);
        } catch (IllegalArgumentException e) {
            final String errMessage = "The optional property '" + ENDPOINT_URL_PROP + "' must be a valid URL if set. " +
                    "URI Syntax Exception is: " + e.getLocalizedMessage();
            LOGGER.error(errMessage);
            LOGGER.debug("", e);
            throw new ProviderCreationException(errMessage, e);
        }
        return s3EndpointOverride;
    }

    @Override
    public synchronized void createBundleVersion(final BundlePersistenceContext context, final InputStream contentStream)
            throws BundlePersistenceException {
        createOrUpdateBundleVersion(context, contentStream);
    }

    @Override
    public synchronized void updateBundleVersion(final BundlePersistenceContext context, final InputStream contentStream) throws BundlePersistenceException {
        createOrUpdateBundleVersion(context, contentStream);
    }

    private synchronized void createOrUpdateBundleVersion(final BundlePersistenceContext context, final InputStream contentStream)
            throws BundlePersistenceException {
        final String key = getKey(context.getCoordinate());
        LOGGER.debug("Saving bundle version to S3 in bucket '{}' with key '{}'", new Object[]{s3BucketName, key});

        final PutObjectRequest request = PutObjectRequest.builder()
                .bucket(s3BucketName)
                .key(key)
                .build();

        final RequestBody requestBody = RequestBody.fromInputStream(contentStream, context.getSize());
        try {
            s3Client.putObject(request, requestBody);
            LOGGER.debug("Successfully saved bundle version to S3 bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
        } catch (Exception e) {
            throw new BundlePersistenceException("Error saving bundle version to S3 due to: " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized void getBundleVersionContent(final BundleVersionCoordinate versionCoordinate, final OutputStream outputStream)
            throws BundlePersistenceException {
        final String key = getKey(versionCoordinate);
        LOGGER.debug("Retrieving bundle version from S3 bucket '{}' with key '{}'", new Object[]{s3BucketName, key});

        final GetObjectRequest request = GetObjectRequest.builder()
                .bucket(s3BucketName)
                .key(key)
                .build();

        try (final ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request)) {
            IoUtils.copy(response, outputStream);
            LOGGER.debug("Successfully retrieved bundle version from S3 bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
        } catch (Exception e) {
            throw new BundlePersistenceException("Error retrieving bundle version from S3 due to: " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized void deleteBundleVersion(final BundleVersionCoordinate versionCoordinate) throws BundlePersistenceException {
        final String key = getKey(versionCoordinate);
        LOGGER.debug("Deleting bundle version from S3 bucket '{}' with key '{}'", new Object[]{s3BucketName, key});

        final DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(s3BucketName)
                .key(key)
                .build();

        try {
            s3Client.deleteObject(request);
            LOGGER.debug("Successfully deleted bundle version from S3 bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
        } catch (Exception e) {
            throw new BundlePersistenceException("Error deleting bundle version from S3 due to: " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized void deleteAllBundleVersions(final BundleCoordinate bundleCoordinate) throws BundlePersistenceException {
        final String basePrefix = s3KeyPrefix == null ? "" : s3KeyPrefix + "/";
        final String bundlePrefix = getBundlePrefix(bundleCoordinate.getBucketId(), bundleCoordinate.getGroupId(), bundleCoordinate.getArtifactId());

        final String prefix = basePrefix + bundlePrefix;
        LOGGER.debug("Deleting all bundle versions from S3 bucket '{}' with prefix '{}'", new Object[]{s3BucketName, prefix});

        try {
            // List all the objects in the bucket with the given prefix of group/artifact...
            final ListObjectsResponse objectsResponse = s3Client.listObjects(
                    ListObjectsRequest.builder()
                            .bucket(s3BucketName)
                            .prefix(prefix)
                            .build()
            );

            // Now delete each object, might be able to do this more efficiently with bulk delete
            for (final S3Object s3Object : objectsResponse.contents()) {
                final String s3ObjectKey = s3Object.key();
                s3Client.deleteObject(DeleteObjectRequest.builder()
                        .bucket(s3BucketName)
                        .key(s3ObjectKey)
                        .build()
                );
                LOGGER.debug("Successfully object from S3 bucket '{}' with key '{}'", new Object[]{s3BucketName, s3ObjectKey});
            }

            LOGGER.debug("Successfully deleted all bundle versions from S3 bucket '{}' with prefix '{}'", new Object[]{s3BucketName, prefix});
        } catch (Exception e) {
            throw new BundlePersistenceException("Error deleting bundle versions from S3 due to: " + e.getMessage(), e);
        }
    }

    @Override
    public void preDestruction() {
        s3Client.close();
    }

    private String getKey(final BundleVersionCoordinate coordinate) {
        final String bundlePrefix = getBundlePrefix(coordinate.getBucketId(), coordinate.getGroupId(), coordinate.getArtifactId());

        final String sanitizedArtifact = sanitize(coordinate.getArtifactId());
        final String sanitizedVersion = sanitize(coordinate.getVersion());

        final String bundleFileExtension = getBundleFileExtension(coordinate.getType());
        final String bundleFilename = sanitizedArtifact + "-" + sanitizedVersion + bundleFileExtension;

        final String key = bundlePrefix + "/" + sanitizedVersion + "/" + bundleFilename;
        if (s3KeyPrefix == null) {
            return key;
        } else {
            return s3KeyPrefix + "/" + key;
        }
    }

    private String getBundlePrefix(final String bucketId, final String groupId, final String artifactId) {
        final String sanitizedBucketId = sanitize(bucketId);
        final String sanitizedGroup = sanitize(groupId);
        final String sanitizedArtifact = sanitize(artifactId);
        return sanitizedBucketId + "/" + sanitizedGroup + "/" + sanitizedArtifact;
    }

    private static String sanitize(final String input) {
        return FileUtils.sanitizeFilename(input).trim().toLowerCase();
    }

    static String getBundleFileExtension(final BundleVersionType bundleType) {
        switch (bundleType) {
            case NIFI_NAR:
                return NAR_EXTENSION;
            case MINIFI_CPP:
                return CPP_EXTENSION;
            default:
                LOGGER.warn("Unknown bundle type: " + bundleType);
                return "";
        }
    }
}
