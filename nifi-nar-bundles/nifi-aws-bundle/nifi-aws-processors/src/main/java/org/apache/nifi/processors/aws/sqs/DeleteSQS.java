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
package org.apache.nifi.processors.aws.sqs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;

@SupportsBatching
@SeeAlso({GetSQS.class, PutSQS.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "AWS", "SQS", "Queue", "Delete"})
@CapabilityDescription("Deletes a message from an Amazon Simple Queuing Service Queue")
public class DeleteSQS extends AbstractSQSProcessor {

    public static final PropertyDescriptor RECEIPT_HANDLE = new PropertyDescriptor.Builder()
            .name("Receipt Handle")
            .description("The identifier that specifies the receipt of the message")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${sqs.receipt.handle}")
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(QUEUE_URL, RECEIPT_HANDLE, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE,
                    REGION, TIMEOUT, ENDPOINT_OVERRIDE, PROXY_HOST, PROXY_HOST_PORT, PROXY_USERNAME, PROXY_PASSWORD));

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

        final String queueUrl = context.getProperty(QUEUE_URL).evaluateAttributeExpressions(flowFile).getValue();

        final AmazonSQSClient client = getClient();
        final DeleteMessageBatchRequest request = new DeleteMessageBatchRequest();
        request.setQueueUrl(queueUrl);

        final List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>();
        final DeleteMessageBatchRequestEntry entry = new DeleteMessageBatchRequestEntry();
        String receiptHandle = context.getProperty(RECEIPT_HANDLE).evaluateAttributeExpressions(flowFile).getValue();
        entry.setReceiptHandle(receiptHandle);
        String entryId = flowFile.getAttribute(CoreAttributes.UUID.key());
        entry.setId(entryId);
        entries.add(entry);
        request.setEntries(entries);

        try {
            DeleteMessageBatchResult response = client.deleteMessageBatch(request);

            // check for errors
            if (!response.getFailed().isEmpty()) {
                throw new ProcessException(response.getFailed().get(0).toString());
            }

            getLogger().info("Successfully deleted message from SQS for {}", new Object[] { flowFile });
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Failed to delete message from SQS due to {}", new Object[] { e });
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
    }

}
