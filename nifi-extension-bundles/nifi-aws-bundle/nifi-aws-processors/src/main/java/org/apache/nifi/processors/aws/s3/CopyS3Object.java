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
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;

import static org.apache.nifi.processors.aws.util.RegionUtilV1.S3_REGION;

@Tags({"Amazon", "S3", "AWS", "Archive", "Copy"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Copies a file from one bucket and key to another in AWS S3")
@SeeAlso({PutS3Object.class, DeleteS3Object.class, ListS3.class, TagS3Object.class, DeleteS3Object.class, FetchS3Object.class})
public class CopyS3Object extends AbstractS3Processor {
    public static final PropertyDescriptor SOURCE_BUCKET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BUCKET_WITH_DEFAULT_VALUE)
            .name("Source Bucket")
            .displayName("Source Bucket")
            .description("The bucket that contains the file to be copied.")
            .build();
    public static final PropertyDescriptor DESTINATION_BUCKET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BUCKET_WITHOUT_DEFAULT_VALUE)
            .name("Destination Bucket")
            .displayName("Destination Bucket")
            .description("The bucket that will receive the copy.")
            .build();

    public static final PropertyDescriptor SOURCE_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(KEY)
            .name("Source Key")
            .displayName("Source Key")
            .description("The source key in the source bucket")
            .build();

    public static final PropertyDescriptor DESTINATION_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(KEY)
            .name("Destination Key")
            .displayName("Destination Key")
            .description("The target key in the target bucket")
            .defaultValue("")
            .build();

    public static final List<PropertyDescriptor> properties = List.of(
            SOURCE_BUCKET,
            SOURCE_KEY,
            DESTINATION_BUCKET,
            DESTINATION_KEY,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            S3_REGION,
            TIMEOUT,
            FULL_CONTROL_USER_LIST,
            READ_USER_LIST,
            WRITE_USER_LIST,
            READ_ACL_LIST,
            WRITE_ACL_LIST,
            CANNED_ACL,
            OWNER,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE,
            SIGNER_OVERRIDE,
            S3_CUSTOM_SIGNER_CLASS_NAME,
            S3_CUSTOM_SIGNER_MODULE_LOCATION,
            PROXY_CONFIGURATION_SERVICE);

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

        final AmazonS3Client s3;
        try {
            s3 = getS3Client(context, flowFile.getAttributes());
        } catch (Exception e) {
            getLogger().error("Failed to initialize S3 client", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final String sourceBucket = context.getProperty(SOURCE_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String sourceKey = context.getProperty(SOURCE_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String targetBucket = context.getProperty(DESTINATION_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String targetKey = context.getProperty(DESTINATION_KEY).evaluateAttributeExpressions(flowFile).getValue();

        try {
            CopyObjectRequest request = new CopyObjectRequest(sourceBucket, sourceKey, targetBucket, targetKey);
            AccessControlList acl = createACL(context, flowFile);
            if (acl != null) {
                request.setAccessControlList(acl);
            }
            CannedAccessControlList cannedAccessControlList = createCannedACL(context, flowFile);

            if (cannedAccessControlList != null) {
                request.setCannedAccessControlList(cannedAccessControlList);
            }

            s3.copyObject(request);
            session.getProvenanceReporter().send(flowFile, getTransitUrl(targetBucket, targetKey));

            session.transfer(flowFile, REL_SUCCESS);
        } catch (final AmazonClientException e) {
            flowFile = extractExceptionDetails(e, session, flowFile);
            getLogger().error("Failed to copy S3 object", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private String getTransitUrl(String targetBucket, String targetKey) {
        String spacer = targetKey.startsWith("/") ? "" : "/";
        return String.format("s3://%s%s%s", targetBucket, spacer, targetKey);
    }
}
