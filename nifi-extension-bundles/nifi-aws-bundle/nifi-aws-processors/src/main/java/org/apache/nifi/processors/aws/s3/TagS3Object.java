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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.nifi.processors.aws.region.RegionUtil.CUSTOM_REGION_WITH_FF_EL;
import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;
import static org.apache.nifi.processors.aws.s3.util.S3Util.getResourceUrl;
import static org.apache.nifi.processors.aws.s3.util.S3Util.nullIfBlank;

@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = "s3.tag.___", description = "The tags associated with the S3 object will be " +
                "written as part of the FlowFile attributes"),
        @WritesAttribute(attribute = "s3.exception", description = "The class name of the exception thrown during processor execution"),
        @WritesAttribute(attribute = "s3.additionalDetails", description = "The S3 supplied detail from the failed operation"),
        @WritesAttribute(attribute = "s3.statusCode", description = "The HTTP error code (if available) from the failed operation"),
        @WritesAttribute(attribute = "s3.errorCode", description = "The S3 moniker of the failed operation"),
        @WritesAttribute(attribute = "s3.errorMessage", description = "The S3 exception message from the failed operation")})
@SeeAlso({PutS3Object.class, FetchS3Object.class, ListS3.class, CopyS3Object.class, GetS3ObjectMetadata.class, GetS3ObjectTags.class, DeleteS3Object.class})
@Tags({"Amazon", "S3", "AWS", "Archive", "Tag"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Adds or updates a tag on an Amazon S3 Object.")
public class TagS3Object extends AbstractS3Processor {

    public static final PropertyDescriptor TAG_KEY = new PropertyDescriptor.Builder()
            .name("Tag Key")
            .description("The key of the tag that will be set on the S3 Object")
            .addValidator(new StandardValidators.StringLengthValidator(1, 127))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor TAG_VALUE = new PropertyDescriptor.Builder()
            .name("Tag Value")
            .description("The value of the tag that will be set on the S3 Object")
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor APPEND_TAG = new PropertyDescriptor.Builder()
            .name("Append Tag")
            .description("If set to true, the tag will be appended to the existing set of tags on the S3 object. " +
                    "Any existing tags with the same key as the new tag will be updated with the specified value. If " +
                    "set to false, the existing tags will be removed and the new tag will be set on the S3 object.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("true")
            .build();

    static final PropertyDescriptor VERSION_ID = new PropertyDescriptor.Builder().fromPropertyDescriptor(AbstractS3Processor.VERSION_ID)
            .description("The Version of the Object to tag")
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BUCKET_WITH_DEFAULT_VALUE,
            KEY,
            REGION,
            CUSTOM_REGION_WITH_FF_EL,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            TAG_KEY,
            TAG_VALUE,
            APPEND_TAG,
            VERSION_ID,
            TIMEOUT,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE,
            PROXY_CONFIGURATION_SERVICE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
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

        final long startNanos = System.nanoTime();

        final String bucket = context.getProperty(BUCKET_WITH_DEFAULT_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String newTagKey = context.getProperty(TAG_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String newTagVal = context.getProperty(TAG_VALUE).evaluateAttributeExpressions(flowFile).getValue();

        if (StringUtils.isBlank(bucket)) {
            failFlowWithBlankEvaluatedProperty(session, flowFile, BUCKET_WITH_DEFAULT_VALUE);
            return;
        }

        if (StringUtils.isBlank(key)) {
            failFlowWithBlankEvaluatedProperty(session, flowFile, KEY);
            return;
        }

        if (StringUtils.isBlank(newTagKey)) {
            failFlowWithBlankEvaluatedProperty(session, flowFile, TAG_KEY);
            return;
        }

        if (StringUtils.isBlank(newTagVal)) {
            failFlowWithBlankEvaluatedProperty(session, flowFile, TAG_VALUE);
            return;
        }

        final String version = context.getProperty(VERSION_ID).evaluateAttributeExpressions(flowFile).getValue();

        final List<Tag> tags = new ArrayList<>();

        try {
            if (context.getProperty(APPEND_TAG).asBoolean()) {
                final GetObjectTaggingRequest getRequest = GetObjectTaggingRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .versionId(nullIfBlank(version))
                        .build();
                final GetObjectTaggingResponse getResponse = client.getObjectTagging(getRequest);

                // preserve tags on S3 object, but filter out existing tag keys that match the one we're setting
                getResponse.tagSet().stream()
                        .filter(t -> !t.key().equals(newTagKey))
                        .forEach(tags::add);
            }

            tags.add(Tag.builder()
                    .key(newTagKey)
                    .value(newTagVal)
                    .build());

            final PutObjectTaggingRequest putRequest = PutObjectTaggingRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .versionId(nullIfBlank(version))
                    .tagging(Tagging.builder()
                            .tagSet(tags)
                            .build())
                    .build();
            client.putObjectTagging(putRequest);
        } catch (final IllegalArgumentException | SdkException e) {
            flowFile = extractExceptionDetails(e, session, flowFile);
            getLogger().error("Failed to tag S3 Object for {}; routing to failure", flowFile, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = setTagAttributes(session, flowFile, tags);

        session.transfer(flowFile, REL_SUCCESS);
        final String url = getResourceUrl(client, bucket, key);
        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully tagged S3 Object for {} in {} millis; routing to success", flowFile, transferMillis);
        session.getProvenanceReporter().invokeRemoteProcess(flowFile, url, "Object tagged");
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        config.renameProperty("tag-key", TAG_KEY.getName());
        config.renameProperty("tag-value", TAG_VALUE.getName());
        config.renameProperty("append-tag", APPEND_TAG.getName());
    }

    private void failFlowWithBlankEvaluatedProperty(ProcessSession session, FlowFile flowFile, PropertyDescriptor pd) {
        getLogger().error("{} value is blank after attribute expression language evaluation", pd.getName());
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    private FlowFile setTagAttributes(ProcessSession session, FlowFile flowFile, List<Tag> tags) {
        flowFile = session.removeAllAttributes(flowFile, Pattern.compile("^s3\\.tag\\..*"));

        final Map<String, String> tagAttrs = new HashMap<>();
        tags.forEach(t -> tagAttrs.put("s3.tag." + t.key(), t.value()));
        flowFile = session.putAllAttributes(flowFile, tagAttrs);
        return flowFile;
    }
}
