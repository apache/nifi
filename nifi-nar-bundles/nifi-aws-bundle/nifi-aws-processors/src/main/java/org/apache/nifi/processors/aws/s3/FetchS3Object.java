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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

@SupportsBatching
@SeeAlso({PutS3Object.class, DeleteS3Object.class, ListS3.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "S3", "AWS", "Get", "Fetch"})
@CapabilityDescription("Retrieves the contents of an S3 Object and writes it to the content of a FlowFile")
@WritesAttributes({
    @WritesAttribute(attribute = "s3.bucket", description = "The name of the S3 bucket"),
    @WritesAttribute(attribute = "path", description = "The path of the file"),
    @WritesAttribute(attribute = "absolute.path", description = "The path of the file"),
    @WritesAttribute(attribute = "filename", description = "The name of the file"),
    @WritesAttribute(attribute = "hash.value", description = "The MD5 sum of the file"),
    @WritesAttribute(attribute = "hash.algorithm", description = "MD5"),
    @WritesAttribute(attribute = "mime.type", description = "If S3 provides the content type/MIME type, this attribute will hold that file"),
    @WritesAttribute(attribute = "s3.etag", description = "The ETag that can be used to see if the file has changed"),
    @WritesAttribute(attribute = "s3.expirationTime", description = "If the file has an expiration date, this attribute will be set, containing the milliseconds since epoch in UTC time"),
    @WritesAttribute(attribute = "s3.expirationTimeRuleId", description = "The ID of the rule that dictates this object's expiration time"),
    @WritesAttribute(attribute = "s3.sseAlgorithm", description = "The server side encryption algorithm of the object"),
    @WritesAttribute(attribute = "s3.version", description = "The version of the S3 object"),})
public class FetchS3Object extends AbstractS3Processor {

    public static final PropertyDescriptor VERSION_ID = new PropertyDescriptor.Builder()
            .name("Version")
            .description("The Version of the Object to download")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(BUCKET, KEY, REGION, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, VERSION_ID,
                SSL_CONTEXT_SERVICE, ENDPOINT_OVERRIDE, SIGNER_OVERRIDE, PROXY_CONFIGURATION_SERVICE, PROXY_HOST, PROXY_HOST_PORT, PROXY_USERNAME, PROXY_PASSWORD));

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
        final String versionId = context.getProperty(VERSION_ID).evaluateAttributeExpressions(flowFile).getValue();

        final AmazonS3 client = getClient();
        final GetObjectRequest request;
        if (versionId == null) {
            request = new GetObjectRequest(bucket, key);
        } else {
            request = new GetObjectRequest(bucket, key, versionId);
        }

        final Map<String, String> attributes = new HashMap<>();
        try (final S3Object s3Object = client.getObject(request)) {
            flowFile = session.importFrom(s3Object.getObjectContent(), flowFile);
            attributes.put("s3.bucket", s3Object.getBucketName());

            final ObjectMetadata metadata = s3Object.getObjectMetadata();
            if (metadata.getContentDisposition() != null) {
                final String fullyQualified = metadata.getContentDisposition();
                final int lastSlash = fullyQualified.lastIndexOf("/");
                if (lastSlash > -1 && lastSlash < fullyQualified.length() - 1) {
                    attributes.put(CoreAttributes.PATH.key(), fullyQualified.substring(0, lastSlash));
                    attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), fullyQualified);
                    attributes.put(CoreAttributes.FILENAME.key(), fullyQualified.substring(lastSlash + 1));
                } else {
                    attributes.put(CoreAttributes.FILENAME.key(), metadata.getContentDisposition());
                }
            }
            if (metadata.getContentMD5() != null) {
                attributes.put("hash.value", metadata.getContentMD5());
                attributes.put("hash.algorithm", "MD5");
            }
            if (metadata.getContentType() != null) {
                attributes.put(CoreAttributes.MIME_TYPE.key(), metadata.getContentType());
            }
            if (metadata.getETag() != null) {
                attributes.put("s3.etag", metadata.getETag());
            }
            if (metadata.getExpirationTime() != null) {
                attributes.put("s3.expirationTime", String.valueOf(metadata.getExpirationTime().getTime()));
            }
            if (metadata.getExpirationTimeRuleId() != null) {
                attributes.put("s3.expirationTimeRuleId", metadata.getExpirationTimeRuleId());
            }
            if (metadata.getUserMetadata() != null) {
                attributes.putAll(metadata.getUserMetadata());
            }
            if (metadata.getSSEAlgorithm() != null) {
                attributes.put("s3.sseAlgorithm", metadata.getSSEAlgorithm());
            }
            if (metadata.getVersionId() != null) {
                attributes.put("s3.version", metadata.getVersionId());
            }
        } catch (final IOException | AmazonClientException ioe) {
            getLogger().error("Failed to retrieve S3 Object for {}; routing to failure", new Object[]{flowFile, ioe});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (!attributes.isEmpty()) {
            flowFile = session.putAllAttributes(flowFile, attributes);
        }

        session.transfer(flowFile, REL_SUCCESS);
        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully retrieved S3 Object for {} in {} millis; routing to success", new Object[]{flowFile, transferMillis});
        session.getProvenanceReporter().fetch(flowFile, "http://" + bucket + ".amazonaws.com/" + key, transferMillis);
    }

}
