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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StringUtils;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.apache.nifi.processors.aws.region.RegionUtil.CUSTOM_REGION_WITH_FF_EL;
import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;
import static org.apache.nifi.processors.aws.s3.util.S3Util.createRangeSpec;

@Tags({"Amazon", "S3", "AWS", "Archive", "Copy"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Copies a file from one bucket and key to another in AWS S3")
@SeeAlso({PutS3Object.class, DeleteS3Object.class, ListS3.class, TagS3Object.class, DeleteS3Object.class, FetchS3Object.class, GetS3ObjectMetadata.class, GetS3ObjectTags.class})
public class CopyS3Object extends AbstractS3Processor {
    public static final long MULTIPART_THRESHOLD = 5L * 1024L * 1024L * 1024L;

    static final PropertyDescriptor SOURCE_BUCKET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BUCKET_WITH_DEFAULT_VALUE)
            .name("Source Bucket")
            .description("The bucket that contains the file to be copied.")
            .build();

    static final PropertyDescriptor SOURCE_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(KEY)
            .name("Source Key")
            .description("The source key in the source bucket")
            .build();

    static final PropertyDescriptor DESTINATION_BUCKET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BUCKET_WITHOUT_DEFAULT_VALUE)
            .name("Destination Bucket")
            .description("The bucket that will receive the copy.")
            .build();

    static final PropertyDescriptor DESTINATION_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(KEY)
            .name("Destination Key")
            .description("The target key in the target bucket")
            .defaultValue("${filename}-1")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SOURCE_BUCKET,
            SOURCE_KEY,
            DESTINATION_BUCKET,
            DESTINATION_KEY,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            REGION,
            CUSTOM_REGION_WITH_FF_EL,
            TIMEOUT,
            FULL_CONTROL_USER_LIST,
            READ_USER_LIST,
            READ_ACL_LIST,
            WRITE_ACL_LIST,
            CANNED_ACL,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE,
            PROXY_CONFIGURATION_SERVICE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);

        config.removeProperty(OBSOLETE_WRITE_USER_LIST);
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

        final String sourceBucket = context.getProperty(SOURCE_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String sourceKey = context.getProperty(SOURCE_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String destinationBucket = context.getProperty(DESTINATION_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String destinationKey = context.getProperty(DESTINATION_KEY).evaluateAttributeExpressions(flowFile).getValue();



        final AtomicReference<String> multipartIdRef = new AtomicReference<>();
        boolean multipartUploadRequired = false;

        try {
            final HeadObjectRequest sourceMetadataRequest = HeadObjectRequest.builder()
                    .bucket(sourceBucket)
                    .key(sourceKey)
                    .build();
            final HeadObjectResponse sourceMetadataResponse = client.headObject(sourceMetadataRequest);
            final long contentLength = sourceMetadataResponse.contentLength();
            multipartUploadRequired = contentLength > MULTIPART_THRESHOLD;

            if (multipartUploadRequired) {
                copyMultipart(client, context, flowFile, sourceBucket, sourceKey, destinationBucket,
                        destinationKey, multipartIdRef, contentLength);
            } else {
                copyObject(client, context, flowFile, sourceBucket, sourceKey, destinationBucket, destinationKey);
            }
            session.getProvenanceReporter().send(flowFile, getTransitUrl(destinationBucket, destinationKey));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            if (multipartUploadRequired && StringUtils.isNotEmpty(multipartIdRef.get())) {
                try {
                    final AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                            .bucket(destinationBucket)
                            .key(destinationKey)
                            .uploadId(multipartIdRef.get())
                            .build();
                    client.abortMultipartUpload(abortRequest);
                } catch (final S3Exception s3e) {
                    getLogger().warn("Abort Multipart Upload failed for Bucket [{}] Key [{}]", destinationBucket, destinationKey, s3e);
                }
            }

            flowFile = extractExceptionDetails(e, session, flowFile);
            getLogger().error("Failed to copy S3 object from Bucket [{}] Key [{}]", sourceBucket, sourceKey, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /*
     * Sections of this code were derived from example code from the official AWS S3 documentation. Specifically this example:
     * https://github.com/awsdocs/aws-doc-sdk-examples/blob/df606a664bf2f7cfe3abc76c187e024451d0279c/java/example_code/s3/src/main/java/aws/example/s3/LowLevelMultipartCopy.java
     */
    private void copyMultipart(final S3Client client, final ProcessContext context, final FlowFile flowFile,
                               final String sourceBucket, final String sourceKey,
                               final String destinationBucket, final String destinationKey, final AtomicReference<String> multipartIdRef,
                               final long contentLength) {
        final CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
                .bucket(destinationBucket)
                .key(destinationKey)
                .grantFullControl(getFullControlGranteeSpec(context, flowFile))
                .grantRead(getReadGranteeSpec(context, flowFile))
                .grantReadACP(getReadACPGranteeSpec(context, flowFile))
                .grantWriteACP(getWriteACPGranteeSpec(context, flowFile))
                .acl(createCannedACL(context, flowFile))
                .build();

        final CreateMultipartUploadResponse createResponse = client.createMultipartUpload(createRequest);

        multipartIdRef.set(createResponse.uploadId());

        long bytePosition = 0;
        int partNumber = 1;
        final List<UploadPartCopyResponse> copyResponses = new ArrayList<>();
        while (bytePosition < contentLength) {
            long lastByte = Math.min(bytePosition + MULTIPART_THRESHOLD - 1, contentLength - 1);

            final UploadPartCopyRequest copyRequest = UploadPartCopyRequest.builder()
                    .sourceBucket(sourceBucket)
                    .sourceKey(sourceKey)
                    .destinationBucket(destinationBucket)
                    .destinationKey(destinationKey)
                    .uploadId(createResponse.uploadId())
                    .copySourceRange(createRangeSpec(bytePosition, lastByte))
                    .partNumber(partNumber++)
                    .build();

            doRetryLoop(request -> copyResponses.add(client.uploadPartCopy((UploadPartCopyRequest) request)), copyRequest);

            bytePosition += MULTIPART_THRESHOLD;
        }

        final List<CompletedPart> completedParts = IntStream.range(0, copyResponses.size())
                .mapToObj(i -> CompletedPart.builder()
                        .partNumber(i + 1)
                        .eTag(copyResponses.get(i).copyPartResult().eTag())
                        .build())
                .toList();

        final CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                .bucket(destinationBucket)
                .key(destinationKey)
                .uploadId(createResponse.uploadId())
                .multipartUpload(CompletedMultipartUpload.builder()
                        .parts(completedParts)
                        .build())
                .build();
        doRetryLoop(complete -> client.completeMultipartUpload(completeRequest), completeRequest);
    }

    private void doRetryLoop(Consumer<SdkRequest> consumer, SdkRequest request) {
        boolean requestComplete = false;
        int retryIndex = 0;

        while (!requestComplete) {
            try {
                consumer.accept(request);
                requestComplete = true;
            } catch (S3Exception e) {
                if (e.statusCode() == 503 && retryIndex < 3) {
                    retryIndex++;
                } else {
                    throw e;
                }
            }
        }
    }

    private void copyObject(final S3Client client, final ProcessContext context,
                               final FlowFile flowFile,
                               final String sourceBucket, final String sourceKey,
                               final String destinationBucket, final String destinationKey) {
        final CopyObjectRequest request = CopyObjectRequest.builder()
                .sourceBucket(sourceBucket)
                .sourceKey(sourceKey)
                .destinationBucket(destinationBucket)
                .destinationKey(destinationKey)
                .grantFullControl(getFullControlGranteeSpec(context, flowFile))
                .grantRead(getReadGranteeSpec(context, flowFile))
                .grantReadACP(getReadACPGranteeSpec(context, flowFile))
                .grantWriteACP(getWriteACPGranteeSpec(context, flowFile))
                .acl(createCannedACL(context, flowFile))
                .build();

        client.copyObject(request);
    }

    private String getTransitUrl(final String destinationBucket, final String destinationKey) {
        final String spacer = destinationKey.startsWith("/") ? "" : "/";
        return String.format("s3://%s%s%s", destinationBucket, spacer, destinationKey);
    }
}
