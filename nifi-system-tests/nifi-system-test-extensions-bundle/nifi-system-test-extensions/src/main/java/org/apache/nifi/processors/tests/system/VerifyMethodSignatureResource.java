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

import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.connector.components.ComponentState;
import org.apache.nifi.components.connector.components.ConnectorMethod;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.tests.system.dynamicclasspath.DynamicallyLoadedType;

import java.util.List;

/**
 * Processor whose declared methods include a private method that returns a type available only through an additional
 * classpath resource. Discovering any {@code @ConnectorMethod} on this processor forces the JVM to resolve every
 * declared method signature, so the additional classpath resource must be present even to invoke the unrelated,
 * constant-returning {@code returnConstant} method.
 */
@RequiresInstanceClassLoading
public class VerifyMethodSignatureResource extends AbstractProcessor {

    public static final PropertyDescriptor CLASSPATH_RESOURCE = new PropertyDescriptor.Builder()
            .name("Classpath Resource")
            .description("An external resource to add to the processor classpath")
            .required(false)
            .dynamicallyModifiesClasspath(true)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(CLASSPATH_RESOURCE);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    }

    @ConnectorMethod(
            name = "returnConstant",
            description = "Returns a constant value without referencing any dynamically loaded class",
            allowedStates = {ComponentState.STOPPED, ComponentState.PROCESSOR_DISABLED}
    )
    public String returnConstant() {
        return "success";
    }

    @SuppressWarnings("unused")
    private DynamicallyLoadedType dynamicallyLoadedMethod() {
        return null;
    }
}
