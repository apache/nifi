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
package org.apache.nifi.service.cql.it;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;

import java.util.Collections;
import java.util.List;

/**
 * Mock processor for exercising a {@link CQLExecutionService} controller service via {@code TestRunner},
 * independent of which backend implementation (Cassandra, ScyllaDB) is under test.
 */
public class MockCqlProcessor extends AbstractProcessor {
    private static final PropertyDescriptor CQL_SESSION_PROVIDER = new PropertyDescriptor.Builder()
            .name("CQL Session Provider")
            .required(true)
            .description("Controller Service to obtain a CQL connection session")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesControllerService(CQLExecutionService.class)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(CQL_SESSION_PROVIDER);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    }
}
