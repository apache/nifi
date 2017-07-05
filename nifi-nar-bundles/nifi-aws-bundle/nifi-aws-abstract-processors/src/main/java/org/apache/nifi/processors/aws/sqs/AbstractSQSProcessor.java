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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;

public abstract class AbstractSQSProcessor extends AbstractAWSCredentialsProviderProcessor<AmazonSQSClient> {

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of messages to send in a single network request")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1L, 10L, true))
            .defaultValue("10")
            .build();

    public static final PropertyDescriptor QUEUE_URL = new PropertyDescriptor.Builder()
            .name("Queue URL")
            .description("The URL of the queue to act upon")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    /**
     * Create client using credentials provider. This is the preferred way for creating clients
     */
    @Override
    protected AmazonSQSClient createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config) {
        getLogger().info("Creating client using aws credentials provider ");

        return new AmazonSQSClient(credentialsProvider, config);
    }

    /**
     * Create client using AWSCredentials
     *
     * @deprecated use {@link #createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)} instead
     */
    @Deprecated
    @Override
    protected AmazonSQSClient createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        getLogger().info("Creating client using aws credentials ");

        return new AmazonSQSClient(credentials, config);
    }

    protected class UrlFlowFileFilter implements FlowFileFilter {

        int count = 0;
        int batchSize;
        String expectedUrl;
        ProcessContext context;

        public UrlFlowFileFilter(int batchSize, String expectedUrl, ProcessContext context) {
            this.batchSize = batchSize;
            this.expectedUrl = expectedUrl;
            this.context = context;
        }

        @Override
        public FlowFileFilterResult filter(FlowFile flowFile) {
            if(count >= batchSize - 1)  {
                return FlowFileFilterResult.REJECT_AND_TERMINATE;
            }
            if(expectedUrl.equals(context.getProperty(QUEUE_URL).evaluateAttributeExpressions(flowFile).getValue())) {
                count++;
                return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
            } else {
                return FlowFileFilterResult.REJECT_AND_CONTINUE;
            }
        }

    }

}
