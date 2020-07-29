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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = "s3.tag.___", description = "The tags associated with the S3 object will be " +
                "written as part of the FlowFile attributes")})
@SeeAlso({PutS3Object.class, FetchS3Object.class, ListS3.class})
@Tags({"Amazon", "S3", "AWS", "Archive", "Tag"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sets tags on a FlowFile within an Amazon S3 Bucket. " +
        "If attempting to tag a file that does not exist, FlowFile is routed to success.")
public class TagS3Object extends AbstractS3Processor {

    public static final PropertyDescriptor TAG_KEY = new PropertyDescriptor.Builder()
            .name("tag-key")
            .displayName("Tag Key")
            .description("The key of the tag that will be set on the S3 Object")
            .addValidator(new StandardValidators.StringLengthValidator(1, 127))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor TAG_VALUE = new PropertyDescriptor.Builder()
            .name("tag-value")
            .displayName("Tag Value")
            .description("The value of the tag that will be set on the S3 Object")
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor APPEND_TAG = new PropertyDescriptor.Builder()
            .name("append-tag")
            .displayName("Append Tag")
            .description("If set to true, the tag will be appended to the existing set of tags on the S3 object. " +
                    "Any existing tags with the same key as the new tag will be updated with the specified value. If " +
                    "set to false, the existing tags will be removed and the new tag will be set on the S3 object.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor VERSION_ID = new PropertyDescriptor.Builder()
            .name("version")
            .displayName("Version ID")
            .description("The Version of the Object to tag")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(KEY, BUCKET, VERSION_ID, TAG_KEY, TAG_VALUE, APPEND_TAG, ACCESS_KEY, SECRET_KEY,
                    CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, REGION, TIMEOUT, SSL_CONTEXT_SERVICE,
                    ENDPOINT_OVERRIDE, SIGNER_OVERRIDE, PROXY_CONFIGURATION_SERVICE, PROXY_HOST, PROXY_HOST_PORT,
                    PROXY_USERNAME, PROXY_PASSWORD));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        final String bucket = context.getProperty(BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String newTagKey = context.getProperty(TAG_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String newTagVal = context.getProperty(TAG_VALUE).evaluateAttributeExpressions(flowFile).getValue();

        if(StringUtils.isBlank(bucket)){
            failFlowWithBlankEvaluatedProperty(session, flowFile, BUCKET);
            return;
        }

        if(StringUtils.isBlank(key)){
            failFlowWithBlankEvaluatedProperty(session, flowFile, KEY);
            return;
        }

        if(StringUtils.isBlank(newTagKey)){
            failFlowWithBlankEvaluatedProperty(session, flowFile, TAG_KEY);
            return;
        }

        if(StringUtils.isBlank(newTagVal)){
            failFlowWithBlankEvaluatedProperty(session, flowFile, TAG_VALUE);
            return;
        }

        final String version = context.getProperty(VERSION_ID).evaluateAttributeExpressions(flowFile).getValue();

        final AmazonS3 s3 = getClient();

        SetObjectTaggingRequest r;
        List<Tag> tags = new ArrayList<>();

        try {
            if(context.getProperty(APPEND_TAG).asBoolean()) {
                final GetObjectTaggingRequest gr = new GetObjectTaggingRequest(bucket, key);
                GetObjectTaggingResult res = s3.getObjectTagging(gr);

                // preserve tags on S3 object, but filter out existing tag keys that match the one we're setting
                tags = res.getTagSet().stream().filter(t -> !t.getKey().equals(newTagKey)).collect(Collectors.toList());
            }

            tags.add(new Tag(newTagKey, newTagVal));

            if(StringUtils.isBlank(version)){
                r = new SetObjectTaggingRequest(bucket, key, new ObjectTagging(tags));
            } else{
                r = new SetObjectTaggingRequest(bucket, key, version, new ObjectTagging(tags));
            }
            s3.setObjectTagging(r);
        } catch (final AmazonServiceException ase) {
            getLogger().error("Failed to tag S3 Object for {}; routing to failure", new Object[]{flowFile, ase});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = setTagAttributes(session, flowFile, tags);

        session.transfer(flowFile, REL_SUCCESS);
        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully tagged S3 Object for {} in {} millis; routing to success", new Object[]{flowFile, transferMillis});
    }

    private void failFlowWithBlankEvaluatedProperty(ProcessSession session, FlowFile flowFile, PropertyDescriptor pd) {
        getLogger().error("{} value is blank after attribute expression language evaluation", new Object[]{pd.getName()});
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    private FlowFile setTagAttributes(ProcessSession session, FlowFile flowFile, List<Tag> tags) {
        flowFile = session.removeAllAttributes(flowFile, Pattern.compile("^s3\\.tag\\..*"));

        final Map<String, String> tagAttrs = new HashMap<>();
        tags.stream().forEach(t -> tagAttrs.put("s3.tag." + t.getKey(), t.getValue()));
        flowFile = session.putAllAttributes(flowFile, tagAttrs);
        return flowFile;
    }
}
