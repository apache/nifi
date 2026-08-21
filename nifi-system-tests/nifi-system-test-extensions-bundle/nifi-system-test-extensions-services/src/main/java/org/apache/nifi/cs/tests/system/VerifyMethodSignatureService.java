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
package org.apache.nifi.cs.tests.system;

import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.connector.components.ComponentState;
import org.apache.nifi.components.connector.components.ConnectorMethod;
import org.apache.nifi.components.connector.components.MethodArgument;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

/**
 * Controller Service used to verify that additional classpath resources are available during {@code @ConnectorMethod}
 * invocation after the classpath resource is set as a Controller Service property.
 */
@RequiresInstanceClassLoading
public class VerifyMethodSignatureService extends AbstractControllerService {

    public static final PropertyDescriptor CLASSPATH_RESOURCE = new PropertyDescriptor.Builder()
            .name("Classpath Resource")
            .description("An external resource to add to the Controller Service classpath")
            .required(false)
            .dynamicallyModifiesClasspath(true)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(CLASSPATH_RESOURCE);
    }

    @ConnectorMethod(
            name = "loadClass",
            description = "Attempts to load the given class from the Controller Service classpath",
            allowedStates = {ComponentState.STOPPED},
            arguments = {
                    @MethodArgument(name = "className", type = String.class, description = "Fully-qualified class name to load", required = true)
            }
    )
    public String loadClass(final String className) throws ClassNotFoundException {
        final Class<?> clazz = Class.forName(className);
        return clazz.getName();
    }
}
