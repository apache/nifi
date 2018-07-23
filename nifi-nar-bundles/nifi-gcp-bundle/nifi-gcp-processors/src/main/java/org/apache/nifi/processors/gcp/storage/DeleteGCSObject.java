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
package org.apache.nifi.processors.gcp.storage;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.gcp.storage.StorageAttributes.BUCKET_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.BUCKET_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.KEY_DESC;


@SupportsBatching
@Tags({"google cloud", "gcs", "google", "storage", "delete"})
@CapabilityDescription("Deletes objects from a Google Cloud Bucket. " +
        "If attempting to delete a file that does not exist, FlowFile is routed to success.")
@SeeAlso({PutGCSObject.class, FetchGCSObject.class, ListGCSBucket.class})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class DeleteGCSObject extends AbstractGCSProcessor {
    public static final PropertyDescriptor BUCKET = new PropertyDescriptor
            .Builder().name("gcs-bucket")
            .displayName("Bucket")
            .description(BUCKET_DESC)
            .required(true)
            .defaultValue("${" + BUCKET_ATTR + "}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY = new PropertyDescriptor
            .Builder().name("gcs-key")
            .displayName("Key")
            .description(KEY_DESC)
            .required(true)
            .defaultValue("${filename}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor GENERATION = new PropertyDescriptor.Builder()
            .name("gcs-generation")
            .displayName("Generation")
            .description("The generation of the object to be deleted. If null, will use latest version of the object.")
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(BUCKET)
                .add(KEY)
                .add(GENERATION)
                .build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        final String bucket = context.getProperty(BUCKET)
                                   .evaluateAttributeExpressions(flowFile)
                                   .getValue();
        final String key = context.getProperty(KEY)
                                   .evaluateAttributeExpressions(flowFile)
                                   .getValue();

        final Long generation = context.getProperty(GENERATION)
                .evaluateAttributeExpressions(flowFile)
                .asLong();


        final Storage storage = getCloudService();

        // Deletes a key on Google Cloud
        try {
            storage.delete(BlobId.of(bucket, key, generation));
        } catch (Exception e) {
            getLogger().error(e.getMessage(), e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully deleted GCS Object for {} in {} millis; routing to success", new Object[]{flowFile, millis});
    }
}
