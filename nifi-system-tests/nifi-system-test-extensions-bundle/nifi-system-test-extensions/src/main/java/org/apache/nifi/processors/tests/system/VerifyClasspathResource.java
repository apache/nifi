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
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.connector.components.ComponentState;
import org.apache.nifi.components.connector.components.ConnectorMethod;
import org.apache.nifi.components.connector.components.MethodArgument;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;

/**
 * Processor used to verify that additional classpath resources are available during configuration verification
 * and {@code @ConnectorMethod} invocation.
 */
@RequiresInstanceClassLoading
public class VerifyClasspathResource extends AbstractProcessor implements VerifiableProcessor {

    public static final PropertyDescriptor CLASSPATH_RESOURCE = new PropertyDescriptor.Builder()
            .name("Classpath Resource")
            .description("An external resource to add to the processor classpath")
            .required(false)
            .dynamicallyModifiesClasspath(true)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLASS_TO_LOAD = new PropertyDescriptor.Builder()
            .name("Class to Load")
            .description("The fully-qualified class name that must be loadable from the processor classpath")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final String LOAD_CLASS_STEP = "Load Class From Classpath";

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(CLASSPATH_RESOURCE, CLASS_TO_LOAD);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final String classToLoad = context.getProperty(CLASS_TO_LOAD).getValue();
        if (classToLoad == null || classToLoad.isBlank()) {
            return List.of(new ConfigVerificationResult.Builder()
                    .verificationStepName(LOAD_CLASS_STEP)
                    .outcome(Outcome.FAILED)
                    .explanation("Class to Load is not configured")
                    .build());
        }

        try {
            loadClass(classToLoad);
            return List.of(new ConfigVerificationResult.Builder()
                    .verificationStepName(LOAD_CLASS_STEP)
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully loaded class " + classToLoad)
                    .build());
        } catch (final ClassNotFoundException e) {
            return List.of(new ConfigVerificationResult.Builder()
                    .verificationStepName(LOAD_CLASS_STEP)
                    .outcome(Outcome.FAILED)
                    .explanation("Failed to load class " + classToLoad + ": " + e.getMessage())
                    .build());
        }
    }

    @ConnectorMethod(
            name = "loadClass",
            description = "Attempts to load the given class from the processor classpath",
            allowedStates = {ComponentState.STOPPED, ComponentState.PROCESSOR_DISABLED},
            arguments = {
                    @MethodArgument(name = "className", type = String.class, description = "Fully-qualified class name to load", required = true)
            }
    )
    public String loadClass(final String className) throws ClassNotFoundException {
        final Class<?> clazz = Class.forName(className);
        return clazz.getName();
    }
}
