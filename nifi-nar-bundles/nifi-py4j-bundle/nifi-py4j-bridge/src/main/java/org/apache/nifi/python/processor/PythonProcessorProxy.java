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

package org.apache.nifi.python.processor;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class PythonProcessorProxy extends AbstractProcessor {
    private final PythonProcessorBridge bridge;
    private volatile Set<Relationship> cachedRelationships = null;
    private volatile List<PropertyDescriptor> cachedPropertyDescriptors = null;
    private volatile Map<String, PropertyDescriptor> cachedDynamicDescriptors = null;
    private boolean supportsDynamicProperties;
    private volatile boolean running = false;

    protected static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("The original FlowFile will be routed to this relationship when it has been successfully transformed")
        .autoTerminateDefault(true)
        .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("The original FlowFile will be routed to this relationship if it unable to be transformed for some reason")
        .build();
    private static final Set<Relationship> implicitRelationships = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(REL_ORIGINAL, REL_FAILURE)));

    public PythonProcessorProxy(final PythonProcessorBridge bridge) {
        this.bridge = bridge;
        supportsDynamicProperties = bridge.getProcessorAdapter().isDynamicPropertySupported();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        if (running && cachedPropertyDescriptors != null) {
            return this.cachedPropertyDescriptors;
        }

        try {
            final List<PropertyDescriptor> properties = bridge.getProcessorAdapter().getSupportedPropertyDescriptors();
            this.cachedPropertyDescriptors = properties; // cache descriptors in case the processor is updated and the properties can no longer be properly accessed
            return properties;
        } catch (final Exception e) {
            getLogger().warn("Failed to obtain list of Property Descriptors from Python processor {}; returning cached list", this, e);
            final List<PropertyDescriptor> properties = this.cachedPropertyDescriptors;
            return properties == null ? Collections.emptyList() : properties;
        }
    }


    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        try {
            reload();
            return bridge.getProcessorAdapter().customValidate(validationContext);
        } catch (final Exception e) {
            getLogger().warn("Failed to perform validation for Python Processor {}; assuming invalid", this, e);

            return Collections.singleton(new ValidationResult.Builder()
                .subject("Perform Validation")
                .valid(false)
                .explanation("Unable to communicate with Python Processor")
                .build());
        }
    }


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (!isSupportsDynamicPropertyDescriptor()) {
            return null;
        }

        // If running, we know that the descriptors haven't changed since they were cached, so just used the cached value.
        if (cachedDynamicDescriptors != null) {
            return cachedDynamicDescriptors.get(propertyDescriptorName);
        }

        try {
            return bridge.getProcessorAdapter().getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
        } catch (final Exception e) {
            getLogger().warn("Failed to obtain Dynamic Property Descriptor with name {} from Python Processor {}; assuming property is not valid", propertyDescriptorName, this, e);
            return null;
        }
    }

    protected boolean isSupportsDynamicPropertyDescriptor() {
        return supportsDynamicProperties;
    }

    @OnScheduled
    public void cacheRelationships() {
        // Get the Relationships from the Python side. Then make a defensive copy and make that copy immutable.
        // We cache this to avoid having to call into the Python side while the Processor is running. However, once
        // it is stopped, its relationships may change due to properties, etc.
        final Set<Relationship> relationships = fetchRelationshipsFromPythonProcessor();
        this.cachedRelationships = Collections.unmodifiableSet(new HashSet<>(relationships));
    }

    @OnScheduled
    public void cacheDynamicPropertyDescriptors(final ProcessContext context) {
        final Map<String, PropertyDescriptor> dynamicDescriptors = new HashMap<>();

        final Set<PropertyDescriptor> descriptors = context.getProperties().keySet();
        for (final PropertyDescriptor descriptor : descriptors) {
            if (descriptor.isDynamic()) {
                dynamicDescriptors.put(descriptor.getName(), descriptor);
            }
        }

        this.cachedDynamicDescriptors = dynamicDescriptors;
        this.running = true;
    }

    @OnStopped
    public void destroyCachedElements() {
        this.running = false;
        this.cachedRelationships = null;
        this.cachedDynamicDescriptors = null;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> cached = cachedRelationships;
        if (cached != null) {
            return cached;
        }

        return fetchRelationshipsFromPythonProcessor();
    }

    private Set<Relationship> fetchRelationshipsFromPythonProcessor() {
        Set<Relationship> processorRelationships;
        try {
            processorRelationships = bridge.getProcessorAdapter().getRelationships();
        } catch (final Exception e) {
            getLogger().warn("Failed to obtain list of Relationships from Python Processor {}; assuming no explicit relationships", this, e);
            processorRelationships = new HashSet<>();
        }
        processorRelationships.addAll(getImplicitRelationships());
        return processorRelationships;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        reload();
        bridge.getProcessorAdapter().onScheduled(context);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        bridge.getProcessorAdapter().onStopped(context);
    }

    @Override
    public String toString() {
        return "PythonProcessor[type=" + bridge.getProcessorType() + ", id=" + getIdentifier() + "]";
    }

    private void reload() {
        reloadProcessor();
        supportsDynamicProperties = bridge.getProcessorAdapter().isDynamicPropertySupported();
    }

    protected abstract void reloadProcessor();

    protected Set<Relationship> getImplicitRelationships() {
        return implicitRelationships;
    }
}
