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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.model.SSEAlgorithm;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.FlowFileAccessException;
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
    @WritesAttribute(attribute = "s3.version", description = "The version of the S3 object"),
    @WritesAttribute(attribute = "s3.encryptionStrategy", description = "The name of the encryption strategy that was used to store the S3 object (if it is encrypted)"),})
public class FetchS3Object extends AbstractS3Processor {

    public static final PropertyDescriptor VERSION_ID = new PropertyDescriptor.Builder()
            .name("Version")
            .description("The Version of the Object to download")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor REQUESTER_PAYS = new PropertyDescriptor.Builder()
            .name("requester-pays")
            .displayName("Requester Pays")
            .required(true)
            .description("If true, indicates that the requester consents to pay any charges associated with retrieving objects from "
                    + "the S3 bucket.  This sets the 'x-amz-request-payer' header to 'requester'.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues(new AllowableValue("true", "True", "Indicates that the requester consents to pay any charges associated "
                    + "with retrieving objects from the S3 bucket."), new AllowableValue("false", "False", "Does not consent to pay "
                            + "requester charges for retrieving objects from the S3 bucket."))
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor RANGE_START = new PropertyDescriptor.Builder()
            .name("range-start")
            .displayName("Range Start")
            .description("The byte position at which to start reading from the object. An empty value or a value of " +
                    "zero will start reading at the beginning of the object.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor RANGE_LENGTH = new PropertyDescriptor.Builder()
            .name("range-length")
            .displayName("Range Length")
            .description("The number of bytes to download from the object, starting from the Range Start. An empty " +
                    "value or a value that extends beyond the end of the object will read to the end of the object.")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, Long.MAX_VALUE))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(BUCKET, KEY, REGION, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, VERSION_ID,
                SSL_CONTEXT_SERVICE, ENDPOINT_OVERRIDE, SIGNER_OVERRIDE, ENCRYPTION_SERVICE, PROXY_CONFIGURATION_SERVICE, PROXY_HOST,
                PROXY_HOST_PORT, PROXY_USERNAME, PROXY_PASSWORD, REQUESTER_PAYS, RANGE_START, RANGE_LENGTH));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));

        AmazonS3EncryptionService encryptionService = validationContext.getProperty(ENCRYPTION_SERVICE).asControllerService(AmazonS3EncryptionService.class);
        if (encryptionService != null) {
            String strategyName = encryptionService.getStrategyName();
            if (strategyName.equals(AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3) || strategyName.equals(AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS)) {
                problems.add(new ValidationResult.Builder()
                        .subject(ENCRYPTION_SERVICE.getDisplayName())
                        .valid(false)
                        .explanation(encryptionService.getStrategyDisplayName() + " is not a valid encryption strategy for fetching objects. Decryption will be handled automatically " +
                                "during the fetch of S3 objects encrypted with " + encryptionService.getStrategyDisplayName())
                        .build()
                );
            }
        }

        return problems;
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
        final boolean requesterPays = context.getProperty(REQUESTER_PAYS).asBoolean();
        final long rangeStart = (context.getProperty(RANGE_START).isSet() ? context.getProperty(RANGE_START).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue() : 0L);
        final Long rangeLength = (context.getProperty(RANGE_LENGTH).isSet() ? context.getProperty(RANGE_LENGTH).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue() : null);

        final AmazonS3 client = getClient();
        final GetObjectRequest request;
        if (versionId == null) {
            request = new GetObjectRequest(bucket, key);
        } else {
            request = new GetObjectRequest(bucket, key, versionId);
        }
        request.setRequesterPays(requesterPays);
        if (rangeLength != null) {
            request.setRange(rangeStart, rangeStart + rangeLength - 1);
        } else {
            request.setRange(rangeStart);
        }

        final Map<String, String> attributes = new HashMap<>();

        AmazonS3EncryptionService encryptionService = context.getProperty(ENCRYPTION_SERVICE).asControllerService(AmazonS3EncryptionService.class);
        if (encryptionService != null) {
            encryptionService.configureGetObjectRequest(request, new ObjectMetadata());
            attributes.put("s3.encryptionStrategy", encryptionService.getStrategyName());
        }

        try (final S3Object s3Object = client.getObject(request)) {
            if (s3Object == null) {
                throw new IOException("AWS refused to execute this request.");
            }
            flowFile = session.importFrom(s3Object.getObjectContent(), flowFile);
            attributes.put("s3.bucket", s3Object.getBucketName());

            final ObjectMetadata metadata = s3Object.getObjectMetadata();
            if (metadata.getContentDisposition() != null) {
                final String contentDisposition = metadata.getContentDisposition();

                if (contentDisposition.equals(PutS3Object.CONTENT_DISPOSITION_INLINE) || contentDisposition.startsWith("attachment; filename=")) {
                    setFilePathAttributes(attributes, key);
                } else {
                    setFilePathAttributes(attributes, contentDisposition);
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
                String sseAlgorithmName = metadata.getSSEAlgorithm();
                attributes.put("s3.sseAlgorithm", sseAlgorithmName);
                if (sseAlgorithmName.equals(SSEAlgorithm.AES256.getAlgorithm())) {
                    attributes.put("s3.encryptionStrategy", AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3);
                } else if (sseAlgorithmName.equals(SSEAlgorithm.KMS.getAlgorithm())) {
                    attributes.put("s3.encryptionStrategy", AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS);
                }
            }
            if (metadata.getVersionId() != null) {
                attributes.put("s3.version", metadata.getVersionId());
            }
        } catch (final IOException | AmazonClientException ioe) {
            getLogger().error("Failed to retrieve S3 Object for {}; routing to failure", new Object[]{flowFile, ioe});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (final FlowFileAccessException ffae) {
            if (ExceptionUtils.indexOfType(ffae, AmazonClientException.class) != -1) {
                getLogger().error("Failed to retrieve S3 Object for {}; routing to failure", new Object[]{flowFile, ffae});
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
            throw ffae;
        }

        if (!attributes.isEmpty()) {
            flowFile = session.putAllAttributes(flowFile, attributes);
        }

        session.transfer(flowFile, REL_SUCCESS);
        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully retrieved S3 Object for {} in {} millis; routing to success", new Object[]{flowFile, transferMillis});
        session.getProvenanceReporter().fetch(flowFile, "http://" + bucket + ".amazonaws.com/" + key, transferMillis);
    }

    protected void setFilePathAttributes(Map<String, String> attributes, String filePathName) {
        final int lastSlash = filePathName.lastIndexOf("/");
        if (lastSlash > -1 && lastSlash < filePathName.length() - 1) {
            attributes.put(CoreAttributes.PATH.key(), filePathName.substring(0, lastSlash));
            attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), filePathName);
            attributes.put(CoreAttributes.FILENAME.key(), filePathName.substring(lastSlash + 1));
        } else {
            attributes.put(CoreAttributes.FILENAME.key(), filePathName);
        }
    }
}
