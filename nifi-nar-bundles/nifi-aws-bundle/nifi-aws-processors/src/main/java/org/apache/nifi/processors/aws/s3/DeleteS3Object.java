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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteVersionRequest;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;


@SupportsBatching
@SeeAlso({PutS3Object.class, FetchS3Object.class, ListS3.class})
@Tags({"Amazon", "S3", "AWS", "Archive", "Delete"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Deletes FlowFiles on an Amazon S3 Bucket. " +
        "If attempting to delete a file that does not exist, FlowFile is routed to success.")
public class DeleteS3Object extends AbstractS3Processor {

    public static final PropertyDescriptor VERSION_ID = new PropertyDescriptor.Builder()
            .name("Version")
            .description("The Version of the Object to delete")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(KEY, BUCKET, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, REGION, TIMEOUT, VERSION_ID,
                    FULL_CONTROL_USER_LIST, READ_USER_LIST, WRITE_USER_LIST, READ_ACL_LIST, WRITE_ACL_LIST, OWNER,
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

        final AmazonS3 s3 = getClient();

        // Deletes a key on Amazon S3
        try {
            if (versionId == null) {
                final DeleteObjectRequest r = new DeleteObjectRequest(bucket, key);
                // This call returns success if object doesn't exist
                s3.deleteObject(r);
            } else {
                final DeleteVersionRequest r = new DeleteVersionRequest(bucket, key, versionId);
                s3.deleteVersion(r);
            }
        } catch (final AmazonServiceException ase) {
            getLogger().error("Failed to delete S3 Object for {}; routing to failure", new Object[]{flowFile, ase});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully delete S3 Object for {} in {} millis; routing to success", new Object[]{flowFile, transferMillis});
    }
}
