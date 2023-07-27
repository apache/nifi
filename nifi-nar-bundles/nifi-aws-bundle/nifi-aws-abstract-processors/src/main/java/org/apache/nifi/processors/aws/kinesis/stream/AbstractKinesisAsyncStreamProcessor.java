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
package org.apache.nifi.processors.aws.kinesis.stream;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.v2.AbstractAwsAsyncProcessor;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;

/**
 * This class is the base class for kinesis stream processors that use the async KinesisClient
 */
public abstract class AbstractKinesisAsyncStreamProcessor extends AbstractAwsAsyncProcessor<KinesisAsyncClient, KinesisAsyncClientBuilder> {
    public static final PropertyDescriptor KINESIS_STREAM_NAME = new PropertyDescriptor.Builder()
            .name("kinesis-stream-name")
            .displayName("Amazon Kinesis Stream Name")
            .description("The name of Kinesis Stream")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    protected KinesisAsyncClientBuilder createClientBuilder(final ProcessContext context) {
        return KinesisAsyncClient.builder();
    }
}