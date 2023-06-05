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
package org.apache.nifi.processors.aws.sns;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.nifi.processors.aws.v2.AbstractAwsProcessor;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;

public abstract class AbstractSNSProcessor extends AbstractAwsProcessor<SnsClient, SnsClientBuilder> {

    protected static final AllowableValue ARN_TYPE_TOPIC
            = new AllowableValue("Topic ARN", "Topic ARN", "The ARN is the name of a topic");
    protected static final AllowableValue ARN_TYPE_TARGET
            = new AllowableValue("Target ARN", "Target ARN", "The ARN is the name of a particular Target, used to notify a specific subscriber");

    public static final PropertyDescriptor ARN = new PropertyDescriptor.Builder()
            .name("Amazon Resource Name (ARN)")
            .description("The name of the resource to which notifications should be published")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ARN_TYPE = new PropertyDescriptor.Builder()
            .name("ARN Type")
            .description("The type of Amazon Resource Name that is being used.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .allowableValues(ARN_TYPE_TOPIC, ARN_TYPE_TARGET)
            .defaultValue(ARN_TYPE_TOPIC.getValue())
            .build();


    @Override
    protected SnsClientBuilder createClientBuilder(final ProcessContext context) {
        return SnsClient.builder();
    }
}
