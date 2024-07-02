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

import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.processors.aws.util.RegionUtilV1.S3_REGION;

@Tags({"Amazon", "S3", "AWS", "Archive", "Exists"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Check for the existence of a file in S3 without attempting to download it. This processor can be " +
        "used as a router for work flows that need to check on a file in S3 before proceeding with data processing")
@SeeAlso({PutS3Object.class, DeleteS3Object.class, ListS3.class, TagS3Object.class, DeleteS3Object.class, FetchS3Object.class})
public class ExistsS3Object extends AbstractS3Processor {
    public static final List<PropertyDescriptor> properties = List.of(
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
            boolean exists = s3.doesObjectExist(bucket, key);

            session.transfer(flowFile, exists ? REL_FOUND : REL_NOT_FOUND);
        } catch (Exception ex) {
            getLogger().error("There was a problem checking for " + String.format("s3://%s%s", bucket, key), ex);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
