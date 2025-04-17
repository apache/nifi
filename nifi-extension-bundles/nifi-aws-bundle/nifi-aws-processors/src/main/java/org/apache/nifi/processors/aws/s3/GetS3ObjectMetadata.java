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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.s3.api.MetadataTarget;
import org.apache.nifi.util.StringUtils;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.aws.util.RegionUtilV1.S3_REGION;

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
            S3_REGION,
            TIMEOUT,
            FULL_CONTROL_USER_LIST,
            READ_USER_LIST,
            READ_ACL_LIST,
            OWNER,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE,
            SIGNER_OVERRIDE,
            S3_CUSTOM_SIGNER_CLASS_NAME,
            S3_CUSTOM_SIGNER_MODULE_LOCATION,
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final AmazonS3Client s3;
        try {
            s3 = getS3Client(context, flowFile.getAttributes());
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
                final GetObjectMetadataRequest objectMetadataRequest = new GetObjectMetadataRequest(bucket, key, StringUtils.isNotBlank(version) ? version : null);
                final ObjectMetadata objectMetadata = s3.getObjectMetadata(objectMetadataRequest);
                final Map<String, Object> combinedMetadata = new LinkedHashMap<>(objectMetadata.getRawMetadata());
                combinedMetadata.putAll(objectMetadata.getUserMetadata());

                if (MetadataTarget.ATTRIBUTES == metadataTarget) {
                    final Map<String, String> newAttributes = combinedMetadata
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
                                if (value instanceof Date dateValue) {
                                    attributeValue = Long.toString(dateValue.getTime());
                                } else {
                                    attributeValue = value.toString();
                                }
                                return attributeValue;
                            }));

                    flowFile = session.putAllAttributes(flowFile, newAttributes);
                } else if (MetadataTarget.FLOWFILE_BODY == metadataTarget) {
                    flowFile = session.write(flowFile, outputStream -> MAPPER.writeValue(outputStream, combinedMetadata));
                }

                relationship = REL_FOUND;
            } catch (final AmazonS3Exception e) {
                if (e.getStatusCode() == 404) {
                    relationship = REL_NOT_FOUND;
                    flowFile = extractExceptionDetails(e, session, flowFile);
                } else {
                    throw e;
                }
            }

            session.transfer(flowFile, relationship);
        } catch (final IllegalArgumentException | AmazonClientException e) {
            getLogger().error("Failed to get S3 Object Metadata from Bucket [{}] Key [{}] Version [{}]", bucket, key, version, e);
            flowFile = extractExceptionDetails(e, session, flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
