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
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

import java.io.File;
import java.util.List;

/**
 * A simple controller service that has a property with dynamicallyModifiesClasspath(true).
 * Used for testing asset synchronization with services that modify the classpath.
 */
@RequiresInstanceClassLoading
public class ModifyClasspathService extends AbstractControllerService {

    public static final PropertyDescriptor RESOURCES = new PropertyDescriptor.Builder()
            .name("Resources")
            .description("Resources to add to the classpath")
            .required(false)
            .dynamicallyModifiesClasspath(true)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY, ResourceType.URL)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(RESOURCES);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final File resourceFile = context.getProperty(RESOURCES).asResource().asFile();
        if (!resourceFile.exists()) {
            throw new IllegalStateException("Resource file does not exist");
        }
    }
}

