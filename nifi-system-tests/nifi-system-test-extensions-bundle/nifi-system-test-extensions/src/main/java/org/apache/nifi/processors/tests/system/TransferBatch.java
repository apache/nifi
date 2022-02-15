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

package org.apache.nifi.processors.tests.system;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;
import static org.apache.nifi.processor.util.StandardValidators.POSITIVE_INTEGER_VALIDATOR;

public class TransferBatch extends AbstractProcessor {

    static final AllowableValue ROUTE_TO_FAILURE = new AllowableValue("Route to Failure", "Route to Failure",
        "If there are not enough FlowFiles available to satisfy the batch size, whatever is available will be transferred to the 'failure' relationship");
    static final AllowableValue ROLLBACK = new AllowableValue("Rollback", "Rollback",
        "If there are not enough FlowFiles available to satisfy the batch size, no FlowFiles will be transferred");
    static final AllowableValue TRANSFER_AVAILABLE = new AllowableValue("Transfer Available", "Transfer Available",
        "If there are not enough FlowFiles available to satisfy the batch size, whatever is available will be transferred to the 'success' relationship");


    static final PropertyDescriptor BATCH_SIZE = new Builder()
        .name("Batch Size")
        .displayName("Batch Size")
        .description("The number of FlowFiles to transfer at once.")
        .required(true)
        .addValidator(POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .defaultValue("1")
        .build();
    static final PropertyDescriptor INSUFFICIENT_BATCH_SIZE_STRATEGY = new Builder()
        .name("Insufficient Batch Size Strategy")
        .displayName("Insufficient Batch Size Strategy")
        .description("Specifies how to handle the situation in which there are fewer FlowFiles available than the configured Batch Size")
        .required(true)
        .allowableValues(TRANSFER_AVAILABLE, ROUTE_TO_FAILURE, ROLLBACK)
        .defaultValue(TRANSFER_AVAILABLE.getValue())
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(BATCH_SIZE, INSUFFICIENT_BATCH_SIZE_STRATEGY);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final List<FlowFile> flowFiles = session.get(batchSize);

        if (flowFiles.size() < batchSize) {
            final String batchSizeStrategy = context.getProperty(INSUFFICIENT_BATCH_SIZE_STRATEGY).getValue();
            if (batchSizeStrategy.equalsIgnoreCase(ROUTE_TO_FAILURE.getValue())) {
                session.transfer(flowFiles, REL_FAILURE);
            } else if (batchSizeStrategy.equalsIgnoreCase(TRANSFER_AVAILABLE.getValue())) {
                session.transfer(flowFiles, REL_SUCCESS);
            } else {
                session.rollback(false);
            }
        } else {
            session.transfer(flowFiles, REL_SUCCESS);
        }
    }
}
