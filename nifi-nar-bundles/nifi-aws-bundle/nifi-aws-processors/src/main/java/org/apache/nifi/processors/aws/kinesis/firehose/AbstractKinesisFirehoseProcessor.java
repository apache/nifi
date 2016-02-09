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
package org.apache.nifi.processors.aws.kinesis.firehose;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;

public abstract class AbstractKinesisFirehoseProcessor extends AbstractAWSCredentialsProviderProcessor<AmazonKinesisFirehoseClient> {

    public static final PropertyDescriptor KINESIS_FIREHOSE_DELIVERY_STREAM_NAME = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Firehose Delivery Stream Name")
            .description("The name of kinesis firehose delivery stream")
            .expressionLanguageSupported(false)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_BUFFER_INTERVAL = new PropertyDescriptor.Builder()
            .name("Max Buffer Interval")
            .description("Buffering interval for messages (between 60 and 900 seconds).")
            .defaultValue("60")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Max Buffer Size (MB)")
            .description("Buffering size for messages (between 1MB and 128MB).")
            .defaultValue("128")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("Batch size for messages (1-500).")
            .defaultValue("250")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .sensitive(false)
            .build();
    /**
     * Create client using aws credentials provider. This is the preferred way for creating clients
     */
    @Override
    protected AmazonKinesisFirehoseClient createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config) {
        getLogger().info("Creating client using aws credentials provider");
        return new AmazonKinesisFirehoseClient(credentialsProvider, config);
    }

    /**
     * Create client using AWSCredentails
     *
     * @deprecated use {@link #createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)} instead
     */
    @Override
    protected AmazonKinesisFirehoseClient createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        getLogger().info("Creating client using aws credentials");
        return new AmazonKinesisFirehoseClient(credentials, config);
    }
}