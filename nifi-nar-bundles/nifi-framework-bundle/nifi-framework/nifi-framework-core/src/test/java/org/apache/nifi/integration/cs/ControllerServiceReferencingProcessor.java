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
package org.apache.nifi.integration.cs;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ControllerServiceReferencingProcessor extends AbstractProcessor {
    protected static final PropertyDescriptor SERVICE = new PropertyDescriptor.Builder()
        .name("Counter Service")
        .identifiesControllerService(Counter.class)
        .required(true)
        .build();

    protected static final PropertyDescriptor OPTIONAL_SERVICE = new PropertyDescriptor.Builder()
        .name("Optional Service")
        .identifiesControllerService(Counter.class)
        .required(false)
        .build();

    protected static final PropertyDescriptor IGNORED_OPTIONAL_SERVICE = new PropertyDescriptor.Builder()
        .name("Ignored Optional Service")
        .identifiesControllerService(Counter.class)
        .required(false)
        .build();

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(FrameworkIntegrationTest.REL_SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(SERVICE, OPTIONAL_SERVICE, IGNORED_OPTIONAL_SERVICE);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        context.getProperty(SERVICE).asControllerService(Counter.class).increment(1L);

        final Counter optionalCounter = context.getProperty(OPTIONAL_SERVICE).asControllerService(Counter.class);
        if (optionalCounter != null) {
            optionalCounter.increment(1L);
        }
    }
}
