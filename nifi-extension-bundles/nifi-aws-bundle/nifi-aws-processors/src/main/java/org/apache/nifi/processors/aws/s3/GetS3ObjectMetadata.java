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

package org.apache.nifi.processors.aws.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.s3.api.MetadataTarget;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.aws.region.RegionUtil.CUSTOM_REGION_WITH_FF_EL;
import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;
import static org.apache.nifi.processors.aws.s3.util.S3Util.nullIfBlank;
import static org.apache.nifi.processors.aws.s3.util.S3Util.sanitizeETag;

@Tags({"Amazon", "S3", "AWS", "Archive", "Exists"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Check for the existence of an Object in S3 and fetch its Metadata without attempting to download it. " +
        "This processor can be used as a router for workflows that need to check on an Object in S3 before proceeding with data processing")
@SeeAlso({PutS3Object.class, DeleteS3Object.class, ListS3.class, TagS3Object.class, DeleteS3Object.class, FetchS3Object.class, GetS3ObjectTags.class})
public class GetS3ObjectMetadata extends AbstractS3Processor {
    static final PropertyDescriptor METADATA_TARGET = new PropertyDescriptor.Builder()
            .name("Metadata Target")
            .description("This determines where the metadata will be written when found.")
            .addValidator(Validator.VALID)
            .required(true)
            .allowableValues(MetadataTarget.class)
            .defaultValue(MetadataTarget.ATTRIBUTES)
            .build();

    static final PropertyDescriptor ATTRIBUTE_INCLUDE_PATTERN = new PropertyDescriptor.Builder()
            .name("Metadata Attribute Include Pattern")
            .description("""
                    A regular expression pattern to use for determining which object metadata entries are included as FlowFile
                    attributes. This pattern is only applied to the 'found' relationship and will not be used to
                    filter the error attributes in the 'failure' relationship.
                    """
            )
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue(".*")
            .dependsOn(METADATA_TARGET, MetadataTarget.ATTRIBUTES)
            .build();

    static final PropertyDescriptor VERSION_ID = new PropertyDescriptor.Builder().fromPropertyDescriptor(AbstractS3Processor.VERSION_ID)
            .description("The Version of the Object for which to retrieve Metadata")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            METADATA_TARGET,
            ATTRIBUTE_INCLUDE_PATTERN,
            BUCKET_WITH_DEFAULT_VALUE,
            KEY,
            VERSION_ID,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            REGION,
            CUSTOM_REGION_WITH_FF_EL,
            TIMEOUT,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE,
            PROXY_CONFIGURATION_SERVICE
    );

    static final Relationship REL_FOUND = new Relationship.Builder()
            .name("found")
            .description("An object was found in the bucket at the supplied key")
            .build();

    static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("No object was found in the bucket the supplied key")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_FOUND,
            REL_NOT_FOUND,
            REL_FAILURE
    );

    private static final String ATTRIBUTE_FORMAT = "s3.%s";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);

        config.removeProperty(FULL_CONTROL_USER_LIST.getName());
        config.removeProperty(READ_USER_LIST.getName());
        config.removeProperty(READ_ACL_LIST.getName());
        config.removeProperty(OBSOLETE_OWNER);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final S3Client client;
        try {
            client = getClient(context, flowFile.getAttributes());
        } catch (Exception e) {
            getLogger().error("Failed to initialize S3 client", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final String bucket = context.getProperty(BUCKET_WITH_DEFAULT_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String version = context.getProperty(VERSION_ID).evaluateAttributeExpressions(flowFile).getValue();

        final MetadataTarget metadataTarget = context.getProperty(METADATA_TARGET).asAllowableValue(MetadataTarget.class);

        final Pattern attributePattern;
        if (metadataTarget == MetadataTarget.ATTRIBUTES) {
            final String attributeRegex = context.getProperty(ATTRIBUTE_INCLUDE_PATTERN).evaluateAttributeExpressions(flowFile).getValue();
            attributePattern = attributeRegex == null ? null : Pattern.compile(attributeRegex);
        } else {
            attributePattern = null;
        }

        try {
            Relationship relationship;

            try {
                final HeadObjectRequest request = HeadObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .versionId(nullIfBlank(version))
                        .build();
                final HeadObjectResponse response = client.headObject(request);
                final Map<String, Object> metadata = extractMetadata(response);

                if (MetadataTarget.ATTRIBUTES == metadataTarget) {
                    final Map<String, String> newAttributes = metadata
                            .entrySet().stream()
                            .filter(e -> {
                                if (attributePattern == null) {
                                    return true;
                                } else {
                                    return attributePattern.matcher(e.getKey()).find();
                                }
                            })
                            .collect(Collectors.toMap(e -> ATTRIBUTE_FORMAT.formatted(e.getKey()), e -> {
                                final Object value = e.getValue();
                                final String attributeValue;
                                if (value instanceof Instant instantValue) {
                                    attributeValue = Long.toString(instantValue.toEpochMilli());
                                } else {
                                    attributeValue = value.toString();
                                }
                                return attributeValue;
                            }));

                    flowFile = session.putAllAttributes(flowFile, newAttributes);
                } else if (MetadataTarget.FLOWFILE_BODY == metadataTarget) {
                    flowFile = session.write(flowFile, outputStream -> MAPPER.writeValue(outputStream, metadata));
                }

                relationship = REL_FOUND;
            } catch (final S3Exception e) {
                if (e.statusCode() == 404) {
                    relationship = REL_NOT_FOUND;
                    flowFile = extractExceptionDetails(e, session, flowFile);
                } else {
                    throw e;
                }
            }

            session.transfer(flowFile, relationship);
        } catch (final IllegalArgumentException | SdkException e) {
            getLogger().error("Failed to get S3 Object Metadata from Bucket [{}] Key [{}] Version [{}]", bucket, key, version, e);
            flowFile = extractExceptionDetails(e, session, flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Extracts metadata fields from HeadObjectResponse that were available via ObjectMetadata.getRawMetadata() and ObjectMetadata.getUserMetadata() methods in v1 SDK.
     *
     * @param head response object containing various metadata
     * @return map of metadata fields
     */
    private static Map<String, Object> extractMetadata(final HeadObjectResponse head) {
        final Map<String, Object> metadata = new HashMap<>();

        // Common HTTP headers
        if (head.contentLength() != null) {
            metadata.put("Content-Length", head.contentLength());
        }
        if (head.contentType() != null) {
            metadata.put("Content-Type", head.contentType());
        }
        if (head.eTag() != null) {
            metadata.put("ETag", sanitizeETag(head.eTag()));
        }
        if (head.lastModified() != null) {
            metadata.put("Last-Modified", head.lastModified());
        }
        if (head.cacheControl() != null) {
            metadata.put("Cache-Control", head.cacheControl());
        }
        if (head.contentEncoding() != null) {
            metadata.put("Content-Encoding", head.contentEncoding());
        }
        if (head.contentDisposition() != null) {
            metadata.put("Content-Disposition", head.contentDisposition());
        }
        if (head.contentLanguage() != null) {
            metadata.put("Content-Language", head.contentLanguage());
        }
        if (head.acceptRanges() != null) {
            metadata.put("Accept-Ranges", head.acceptRanges());
        }

        // AWS x-amz-* headers
        if (head.serverSideEncryption() != null) {
            metadata.put("x-amz-server-side-encryption", head.serverSideEncryptionAsString());
        }
        if (head.ssekmsKeyId() != null) {
            metadata.put("x-amz-server-side-encryption-aws-kms-key-id", head.ssekmsKeyId());
        }
        if (head.storageClass() != null) {
            metadata.put("x-amz-storage-class", head.storageClassAsString());
        }
        if (head.versionId() != null) {
            metadata.put("x-amz-version-id", head.versionId());
        }
        if (head.tagCount() != null) {
            metadata.put("x-amz-tagging-count", head.tagCount());
        }

        // User-defined x-amz-meta-* metadata
        head.metadata().forEach((key, value) ->
                metadata.put(key.toLowerCase(), value)
        );

        return metadata;
    }
}
