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

import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AsyncLoadedProcessor;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@SupportsSensitiveDynamicProperties
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER},
        description = "Python processors can store and retrieve state using the State Management APIs. Consult the State Manager section of the Developer's Guide for more details.")
public abstract class PythonProcessorProxy<T extends PythonProcessor> extends AbstractProcessor implements AsyncLoadedProcessor {
    private final String processorType;
    private volatile PythonProcessorInitializationContext initContext;
    private volatile PythonProcessorBridge bridge;
    private volatile Set<Relationship> cachedRelationships = null;
    private volatile List<PropertyDescriptor> cachedPropertyDescriptors = null;
    private volatile Map<String, PropertyDescriptor> cachedDynamicDescriptors = null;
    private volatile Boolean supportsDynamicProperties;

    private volatile T currentTransform;
    private volatile PythonProcessorAdapter currentAdapter;
    private volatile ProcessContext currentProcessContext;


    protected static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("The original FlowFile will be routed to this relationship when it has been successfully transformed")
        .autoTerminateDefault(true)
        .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("The original FlowFile will be routed to this relationship if it unable to be transformed for some reason")
        .build();
    private static final Set<Relationship> implicitRelationships = Set.of(
        REL_ORIGINAL,
        REL_FAILURE);

    public PythonProcessorProxy(final String processorType, final Supplier<PythonProcessorBridge> bridgeFactory, final boolean initialize) {
        this.processorType = processorType;

        Thread.ofVirtual().name("Initialize " + processorType).start(() -> {
            this.bridge = bridgeFactory.get();

            if (initialize) {
                // If initialization context has already been set, initialize bridge.
                final PythonProcessorInitializationContext pythonInitContext = initContext;
                if (pythonInitContext != null) {
                    this.bridge.initialize(pythonInitContext);
                }
            }
        });
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final PythonProcessorInitializationContext initContext = new PythonProcessorInitializationContext() {
            @Override
            public String getIdentifier() {
                return context.getIdentifier();
            }

            @Override
            public ComponentLog getLogger() {
                return context.getLogger();
            }
        };

        this.initContext = initContext;

        // If Bridge has already been set, initialize it.
        final PythonProcessorBridge bridge = this.bridge;
        if (bridge != null) {
            bridge.initialize(initContext);
        }
    }


    protected synchronized T getTransform() {
        final PythonProcessorBridge bridge = getBridge().orElseThrow(() -> new IllegalStateException(this + " is not finished initializing"));
        final Optional<PythonProcessorAdapter> optionalAdapter = bridge.getProcessorAdapter();
        if (optionalAdapter.isEmpty()) {
            throw new IllegalStateException(this + " is not finished initializing");
        }

        final PythonProcessorAdapter adapter = optionalAdapter.get();
        if (adapter != currentAdapter) {
            final T transform = (T) adapter.getProcessor();
            transform.setContext(currentProcessContext);

            currentTransform = transform;
            currentAdapter = adapter;
        }

        return currentTransform;
    }

    protected Optional<PythonProcessorBridge> getBridge() {
        return Optional.ofNullable(this.bridge);
    }

    @Override
    public LoadState getState() {
        if (bridge == null) {
            return LoadState.INITIALIZING_ENVIRONMENT;
        }

        return bridge.getLoadState();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        if (cachedPropertyDescriptors != null) {
            return this.cachedPropertyDescriptors;
        }

        if (getState() != LoadState.FINISHED_LOADING) {
            return Collections.emptyList();
        }

        final Optional<PythonProcessorAdapter> optionalAdapter = bridge.getProcessorAdapter();
        if (optionalAdapter.isEmpty()) {
            // If we don't have the adapter yet, use whatever is cached, even if it's old, or an empty List if we have nothing cached.
            return this.cachedPropertyDescriptors == null ? Collections.emptyList() : cachedPropertyDescriptors;
        }

        try {
            final List<PropertyDescriptor> properties = optionalAdapter.get().getSupportedPropertyDescriptors();
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
        if (bridge == null) {
            return List.of(new ValidationResult.Builder()
                .subject("Processor")
                .explanation("Python environment is not yet initialized")
                .valid(false)
                .build());
        }

        final LoadState loadState = bridge.getLoadState();
        if (loadState == LoadState.LOADING_PROCESSOR_CODE || loadState == LoadState.DOWNLOADING_DEPENDENCIES) {
            return List.of(new ValidationResult.Builder()
                .subject("Processor")
                .explanation("Processor has not yet completed initialization")
                .valid(false)
                .build());
        }

        try {
            reload();

            final Optional<PythonProcessorAdapter> optionalAdapter = bridge.getProcessorAdapter();
            if (optionalAdapter.isEmpty()) {
                return List.of(new ValidationResult.Builder()
                    .subject("Processor")
                    .explanation("Processor has not yet completed initialization")
                    .valid(false)
                    .build());
            }

            return optionalAdapter.get().customValidate(validationContext);
        } catch (final Exception e) {
            getLogger().warn("Failed to perform validation for Python Processor {}; assuming invalid", this, e);

            return Collections.singleton(new ValidationResult.Builder()
                .subject("Perform Validation")
                .valid(false)
                .explanation("Failed to trigger Python Processor to perform validation: " + e)
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

        if (getState() != LoadState.FINISHED_LOADING) {
            return null;
        }

        try {
            final Optional<PythonProcessorAdapter> optionalAdapter = bridge.getProcessorAdapter();
            return optionalAdapter.map(adapter -> adapter.getSupportedDynamicPropertyDescriptor(propertyDescriptorName))
                    .orElse(null);
        } catch (final Exception e) {
            getLogger().warn("Failed to obtain Dynamic Property Descriptor with name {} from Python Processor {}; assuming property is not valid", propertyDescriptorName, this, e);
            return null;
        }
    }

    protected boolean isSupportsDynamicPropertyDescriptor() {
        if (this.supportsDynamicProperties != null) {
            return supportsDynamicProperties;
        }

        if (getState() != LoadState.FINISHED_LOADING) {
            return false;
        }

        final Optional<PythonProcessorAdapter> adapter = bridge.getProcessorAdapter();
        final boolean supported = adapter.map(PythonProcessorAdapter::isDynamicPropertySupported).orElse(false);
        supportsDynamicProperties = supported;
        return supported;
    }

    private void cacheRelationships() {
        // Get the Relationships from the Python side. Then make a defensive copy and make that copy immutable.
        // We cache this to avoid having to call into the Python side while the Processor is running. However, once
        // it is stopped, its relationships may change due to properties, etc.
        final Set<Relationship> relationships = fetchRelationshipsFromPythonProcessor();
        this.cachedRelationships = Set.copyOf(relationships);
    }

    private void cacheDynamicPropertyDescriptors(final ProcessContext context) {
        final Map<String, PropertyDescriptor> dynamicDescriptors = new HashMap<>();

        final Set<PropertyDescriptor> descriptors = context.getProperties().keySet();
        for (final PropertyDescriptor descriptor : descriptors) {
            if (descriptor.isDynamic()) {
                dynamicDescriptors.put(descriptor.getName(), descriptor);
            }
        }

        this.cachedDynamicDescriptors = dynamicDescriptors;
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
        if (getState() != LoadState.FINISHED_LOADING) {
            return Collections.emptySet();
        }

        Set<Relationship> processorRelationships;
        try {
            processorRelationships = bridge.getProcessorAdapter()
                .map(PythonProcessorAdapter::getRelationships)
                .orElseGet(HashSet::new);
        } catch (final Exception e) {
            getLogger().warn("Failed to obtain list of Relationships from Python Processor {}; assuming no explicit relationships", this, e);
            processorRelationships = new HashSet<>();
        }

        processorRelationships.addAll(getImplicitRelationships());
        return processorRelationships;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.currentProcessContext = context;

        if (bridge == null) {
            throw new IllegalStateException("Processor is not yet initialized");
        }

        reload();

        final PythonProcessorAdapter adapter = bridge.getProcessorAdapter()
            .orElseThrow(() -> new IllegalStateException("Processor has not finished initializing"));

        adapter.onScheduled(context);

        adapter.getProcessor().setContext(context);

        cacheRelationships();
        cacheDynamicPropertyDescriptors(context);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        if (bridge == null) {
            throw new IllegalStateException("Processor is not yet initialized");
        }

        bridge.getProcessorAdapter()
            .orElseThrow(() -> new IllegalStateException("Processor has not finished initializing"))
            .onStopped(context);
    }

    @Override
    public String toString() {
        return "PythonProcessor[type=" + processorType + ", id=" + getIdentifier() + "]";
    }

    private void reload() {
        if (bridge == null) {
            return;
        }

        final boolean reloaded = bridge.reload();
        if (reloaded) {
            getLogger().info("Successfully reloaded Processor");
        }

        cachedPropertyDescriptors = null;
        cachedRelationships = null;
        supportsDynamicProperties = bridge.getProcessorAdapter()
            .orElseThrow(() -> new IllegalStateException("Processor has not finished initializing"))
            .isDynamicPropertySupported();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        cachedPropertyDescriptors = null;
        cachedRelationships = null;
        super.onPropertyModified(descriptor, oldValue, newValue);
    }

    protected Set<Relationship> getImplicitRelationships() {
        return implicitRelationships;
    }
}