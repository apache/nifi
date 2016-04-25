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
package org.apache.nifi.processors.alluxio;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;

import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;

/**
 * AbstractAlluxioProcessor is a base class for Alluxio processors and contains logic and variables common to most
 * processors integrating with Alluxio.
 */
public abstract class AbstractAlluxioProcessor extends AbstractProcessor {

    public static final PropertyDescriptor MASTER_HOSTNAME = new PropertyDescriptor.Builder()
            .name("alluxio-master-ip")
            .displayName("Master hostname")
            .description("Hostname of the Alluxio File System Master node.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MASTER_PORT = new PropertyDescriptor.Builder()
            .name("alluxio-master-port")
            .displayName("Master port")
            .description("Port to use when connecting to the Alluxio File System Master node.")
            .required(true)
            .defaultValue("19998")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(MASTER_HOSTNAME);
        descriptors.add(MASTER_PORT);
    }

    protected final AtomicReference<FileSystem> fileSystem = new AtomicReference<>(null);

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        createFileSystem(context);
    }

    protected void createFileSystem(ProcessContext context) {
        final String host = context.getProperty(MASTER_HOSTNAME).getValue();
        System.setProperty("alluxio.master.hostname", host);
        System.setProperty("alluxio.master.port", context.getProperty(MASTER_PORT).getValue());
        fileSystem.set(FileSystem.Factory.get());
    }

    protected FlowFile updateFlowFile(URIStatus uriStatus, FlowFile flowFile, ProcessSession session) {
        if (uriStatus != null){
            try {
                Method[] methods = URIStatus.class.getDeclaredMethods();
                Map<String, String> attributes = new HashMap<>();
                for (Method method : methods) {
                    if (Modifier.isPublic(method.getModifiers()) && method.getName().startsWith("get")) {
                        Object propertyValue = method.invoke(uriStatus);
                        if (propertyValue != null) {
                            String propertyName = extractPropertyNameFromMethod(method);
                            attributes.put(propertyName, propertyValue.toString());
                        }
                    }
                }
                flowFile = session.putAllAttributes(flowFile, attributes);
            } catch (Exception e) {
                getLogger().warn("Failed to update FlowFile with URI attributes", e);
            }
        }
        return flowFile;
    }

    private String extractPropertyNameFromMethod(Method method) {
        char c[] = method.getName().substring(3).toCharArray();
        c[0] = Character.toLowerCase(c[0]);
        return "alluxio_" + new String(c);
    }

}