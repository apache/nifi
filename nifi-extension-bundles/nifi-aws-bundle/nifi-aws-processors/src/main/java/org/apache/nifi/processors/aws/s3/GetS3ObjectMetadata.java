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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.aws.util.RegionUtilV1.S3_REGION;

@Tags({"Amazon", "S3", "AWS", "Archive", "Exists"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Check for the existence of a file in S3 without attempting to download it. This processor can be " +
        "used as a router for work flows that need to check on a file in S3 before proceeding with data processing")
@SeeAlso({PutS3Object.class, DeleteS3Object.class, ListS3.class, TagS3Object.class, DeleteS3Object.class, FetchS3Object.class})
public class GetS3ObjectMetadata extends AbstractS3Processor {
    public static final AllowableValue MODE_FETCH_METADATA = new AllowableValue("fetch", "Fetch Metadata",
            "This is the default mode. It will fetch the metadata and write it to either the FlowFile content or an " +
                    "attribute");
    public static final AllowableValue MODE_ROUTER = new AllowableValue("router", "Router", "When selected," +
            "this mode will skip writing the metadata and just send the FlowFile to the found or not-found relationship. It should be used " +
            "when the goal is to just route FlowFiles based on whether or not a key is present in S3.");
    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("Mode")
            .allowableValues(MODE_FETCH_METADATA, MODE_ROUTER)
            .defaultValue(MODE_FETCH_METADATA.getValue())
            .required(true)
            .description("Configure the mode of operation for this processor")
            .addValidator(Validator.VALID)
            .build();

    public static final AllowableValue TARGET_ATTRIBUTE = new AllowableValue("attribute", "Attribute", "When " +
            "selected, the metadata will be written to a user-supplied attribute");
    public static final AllowableValue TARGET_FLOWFILE_BODY = new AllowableValue("flowfile-content", "FlowFile Body", "Write " +
            "the metadata to the FlowFile's content as JSON data.");

    public static final PropertyDescriptor METADATA_TARGET = new PropertyDescriptor.Builder()
            .name("Metadata Target")
            .description("This determines where the metadata will be written when it is found.")
            .addValidator(Validator.VALID)
            .required(true)
            .allowableValues(TARGET_ATTRIBUTE, TARGET_FLOWFILE_BODY)
            .defaultValue(TARGET_ATTRIBUTE)
            .dependsOn(MODE, MODE_FETCH_METADATA)
            .build();

    public static final PropertyDescriptor METADATA_ATTRIBUTE_PREFIX = new PropertyDescriptor.Builder()
            .name("Metadata Attribute Prefix")
            .description("The prefix for FlowFile attributes generated from the S3 object metadata.")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .defaultValue("s3.")
            .dependsOn(METADATA_TARGET, TARGET_ATTRIBUTE)
            .required(true)
            .build();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final List<PropertyDescriptor> properties = List.of(
            MODE,
            METADATA_TARGET,
            METADATA_ATTRIBUTE_PREFIX,
            BUCKET_WITH_DEFAULT_VALUE,
            KEY,
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
            PROXY_CONFIGURATION_SERVICE);

    public static Relationship REL_FOUND = new Relationship.Builder()
            .name("found")
            .description("An object was found in the bucket at the supplied key")
            .build();

    public static Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("No object was found in the bucket the supplied key")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(List.of(REL_FOUND, REL_NOT_FOUND, REL_FAILURE));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        boolean isRouter = context.getProperty(MODE).getValue().equals(MODE_ROUTER.getValue());

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

        try {
            Relationship relationship;

            try {
                ObjectMetadata metadata = s3.getObjectMetadata(bucket, key);
                Map<String, Object> combinedMetadata = new HashMap<>(metadata.getRawMetadata());
                combinedMetadata.putAll(metadata.getUserMetadata());

                if (!isRouter && context.getProperty(METADATA_TARGET).getValue().equals(TARGET_ATTRIBUTE.getValue())) {
                    String attributePrefix = context.getProperty(METADATA_ATTRIBUTE_PREFIX).getValue();
                    Map<String, String> newAttributes = combinedMetadata
                            .entrySet().stream()
                            .collect(Collectors.toMap(e -> attributePrefix + e.getKey(), e -> e.getValue().toString()));

                    flowFile = session.putAllAttributes(flowFile, newAttributes);
                } else if (!isRouter && context.getProperty(METADATA_TARGET).getValue().equals(TARGET_FLOWFILE_BODY.getValue())) {
                    String metadataJson = MAPPER.writeValueAsString(combinedMetadata);
                    flowFile = session.write(flowFile, os -> os.write(metadataJson.getBytes(StandardCharsets.UTF_8)));
                }

                relationship = REL_FOUND;
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 404) {
                    relationship = REL_NOT_FOUND;
                    flowFile = extractExceptionDetails(e, session, flowFile);
                } else {
                    throw e;
                }
            }

            session.transfer(flowFile, relationship);
        } catch (final IOException | AmazonClientException e) {
            getLogger().error("Failed to get S3 Object Metadata from s3://{}{} ", bucket, key, e);
            flowFile = extractExceptionDetails(e, session, flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
