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
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;

@SupportsBatching
@SeeAlso({PutS3Object.class})
@Tags({"Amazon", "S3", "AWS", "Archive", "Delete"})
@CapabilityDescription("Deletes FlowFiles on an Amazon S3 Bucket")
public class DeleteS3Object extends AbstractS3Processor {

    public static final PropertyDescriptor VERSION_ID = new PropertyDescriptor.Builder()
            .name("Version")
            .description("The Version of the Object to delete")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(false)
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(KEY, BUCKET, ACCESS_KEY, SECRET_KEY, CREDENTAILS_FILE, REGION, TIMEOUT,
                    FULL_CONTROL_USER_LIST, READ_USER_LIST, WRITE_USER_LIST, READ_ACL_LIST, WRITE_ACL_LIST, OWNER));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        final String bucket = context.getProperty(BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String versionId = context.getProperty(VERSION_ID).evaluateAttributeExpressions(flowFile).getValue();

        final AmazonS3 s3 = getClient();
        final GetObjectRequest request;
        if (versionId == null) {
            request = new GetObjectRequest(bucket, key);
        } else {
            request = new GetObjectRequest(bucket, key, versionId);
        }

        try (final S3Object s3Object = s3.getObject(request)) {
            flowFile = session.importFrom(s3Object.getObjectContent(), flowFile);

            ObjectMetadata metadata = s3Object.getObjectMetadata();
            String objectVersionId = metadata.getVersionId();
            if (objectVersionId == null) {
                final DeleteObjectRequest r = new DeleteObjectRequest(bucket, key);
                s3.deleteObject(r);
            } else {
                final DeleteVersionRequest r = new DeleteVersionRequest(bucket, key, objectVersionId);
                s3.deleteVersion(r);
            }
        } catch (final IOException | AmazonClientException ioe) {
            getLogger().error("Failed to retrieve S3 Object for {}; routing to failure", new Object[]{flowFile, ioe});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully delete S3 Object for {} in {} millis; routing to success", new Object[]{flowFile, transferMillis});
        session.getProvenanceReporter().receive(flowFile, "http://" + bucket + ".amazonaws.com/" + key, transferMillis);
    }
}
