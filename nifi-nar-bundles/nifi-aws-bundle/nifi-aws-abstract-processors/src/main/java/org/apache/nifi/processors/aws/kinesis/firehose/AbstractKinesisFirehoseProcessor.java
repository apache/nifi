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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.v2.AbstractAwsSyncProcessor;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.FirehoseClientBuilder;

/**
 * This class is the base class for Kinesis Firehose processors
 */
public abstract class AbstractKinesisFirehoseProcessor extends AbstractAwsSyncProcessor<FirehoseClient, FirehoseClientBuilder> {

    public static final PropertyDescriptor KINESIS_FIREHOSE_DELIVERY_STREAM_NAME = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Firehose Delivery Stream Name")
            .description("The name of kinesis firehose delivery stream")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("Batch size for messages (1-500).")
            .defaultValue("250")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 500, true))
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_MESSAGE_BUFFER_SIZE_MB = new PropertyDescriptor.Builder()
            .name("Max message buffer size")
            .description("Max message buffer")
            .defaultValue("1 MB")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .sensitive(false)
            .build();

    @Override
    protected FirehoseClientBuilder createClientBuilder(final ProcessContext context) {
        return FirehoseClient.builder();
    }
}