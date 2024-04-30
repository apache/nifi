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
package org.apache.nifi.registry.flow;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class GhostFlowRegistryClient implements FlowRegistryClient {

    public static final String ERROR_MESSAGE = "Unable to instantiate FlowRegistryClient class";

    private final String id;
    private final String canonicalClassName;

    public GhostFlowRegistryClient(final String id, final String canonicalClassName) {
        this.id = id;
        this.canonicalClassName = canonicalClassName;
    }

    @Override
    public void initialize(final FlowRegistryClientInitializationContext context) {

    }

    @Override
    public boolean isStorageLocationApplicable(final FlowRegistryClientConfigurationContext context, final String location) {
        return false;
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext context) {
        return Collections.singleton(new ValidationResult.Builder()
                .input("Any Property")
                .subject("Missing Registry Client")
                .valid(false)
                .explanation("Registry Client is of type " + canonicalClassName + ", but this is not a valid Registry Client type")
                .build());
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(final String propertyName) {
        return new PropertyDescriptor.Builder()
                .name(propertyName)
                .description(propertyName)
                .required(true)
                .sensitive(true)
                .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        return Collections.emptyList();
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public Set<FlowRegistryBucket> getBuckets(final FlowRegistryClientConfigurationContext context, final String branch) throws FlowRegistryException {
        throw new FlowRegistryException(ERROR_MESSAGE);
    }

    @Override
    public FlowRegistryBucket getBucket(final FlowRegistryClientConfigurationContext context, final BucketLocation bucketLocation) throws FlowRegistryException {
        throw new FlowRegistryException(ERROR_MESSAGE);
    }

    @Override
    public RegisteredFlow registerFlow(final FlowRegistryClientConfigurationContext context, final RegisteredFlow flow) throws FlowRegistryException {
        throw new FlowRegistryException(ERROR_MESSAGE);
    }

    @Override
    public RegisteredFlow deregisterFlow(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws FlowRegistryException {
        throw new FlowRegistryException(ERROR_MESSAGE);
    }

    @Override
    public RegisteredFlow getFlow(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws FlowRegistryException {
        throw new FlowRegistryException(ERROR_MESSAGE);
    }

    @Override
    public Set<RegisteredFlow> getFlows(final FlowRegistryClientConfigurationContext context, final BucketLocation bucketLocation) throws FlowRegistryException {
        throw new FlowRegistryException(ERROR_MESSAGE);
    }

    @Override
    public RegisteredFlowSnapshot getFlowContents(final FlowRegistryClientConfigurationContext context, final FlowVersionLocation flowVersionLocation)
            throws FlowRegistryException {
        throw new FlowRegistryException(ERROR_MESSAGE);
    }

    @Override
    public RegisteredFlowSnapshot registerFlowSnapshot(final FlowRegistryClientConfigurationContext context, final RegisteredFlowSnapshot flowSnapshot, final RegisterAction action)
            throws FlowRegistryException {
        throw new FlowRegistryException(ERROR_MESSAGE);
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersions(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws FlowRegistryException {
        throw new FlowRegistryException(ERROR_MESSAGE);
    }

    @Override
    public Optional<String> getLatestVersion(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws FlowRegistryException {
        throw new FlowRegistryException(ERROR_MESSAGE);
    }
}
