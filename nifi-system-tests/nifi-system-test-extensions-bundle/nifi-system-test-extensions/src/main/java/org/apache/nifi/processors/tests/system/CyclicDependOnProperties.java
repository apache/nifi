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

import java.util.Arrays;
import java.util.List;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

public class CyclicDependOnProperties extends AbstractProcessor {
    // causing an intentional situation with cyclic dependencies (duplicate required because Java does not allow forward reference)
    static final PropertyDescriptor CYCLIC_DEPDENDENCY_PROPERTY_DUPLICATE = new Builder()
            .name("Cyclic Dependency Property")
            .displayName("Cyclic Dependency Property")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor CYCLIC_DEPENDENCY_ROOT = new Builder()
            .name("Cyclic Dependency Root")
            .displayName("Cyclic Dependency Root")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .dependsOn(CYCLIC_DEPDENDENCY_PROPERTY_DUPLICATE)
            .build();

    static final PropertyDescriptor CYCLIC_DEPENDENCY_PROPERTY = new Builder()
            .name("Cyclic Dependency Property")
            .displayName("Cyclic Dependency Property")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .dependsOn(CYCLIC_DEPENDENCY_ROOT)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(
                CYCLIC_DEPENDENCY_ROOT,
                CYCLIC_DEPENDENCY_PROPERTY
        );
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    }
}
