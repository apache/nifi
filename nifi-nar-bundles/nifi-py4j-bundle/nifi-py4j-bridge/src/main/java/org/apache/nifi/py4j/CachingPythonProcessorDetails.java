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

package org.apache.nifi.py4j;

import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.python.processor.documentation.MultiProcessorUseCaseDetails;
import org.apache.nifi.python.processor.documentation.PropertyDescription;
import org.apache.nifi.python.processor.documentation.UseCaseDetails;

import java.util.List;

/**
 * A wrapper around a PythonProcessorDetails that caches the results of the delegate's methods.
 * Making calls to the Python side is relatively expensive, and we make many calls to {@link #getProcessorType()},
 * {@link #getProcessorVersion()}, etc. This simple wrapper allows us to make the calls only once.
 */
public class CachingPythonProcessorDetails implements PythonProcessorDetails {
    private final PythonProcessorDetails delegate;
    private volatile String processorType;
    private volatile String processorVersion;
    private volatile String capabilityDescription;
    private volatile String sourceLocation;
    private volatile List<String> tags;
    private volatile List<String> dependencies;
    private volatile String processorInterface;
    private volatile List<UseCaseDetails> useCases;
    private volatile List<MultiProcessorUseCaseDetails> multiProcessorUseCases;
    private volatile List<PropertyDescription> propertyDescriptions;


    public CachingPythonProcessorDetails(final PythonProcessorDetails delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getProcessorType() {
        if (processorType == null) {
            processorType = delegate.getProcessorType();
        }
        return processorType;
    }

    @Override
    public String getProcessorVersion() {
        if (processorVersion == null) {
            processorVersion = delegate.getProcessorVersion();
        }
        return processorVersion;
    }

    @Override
    public String getSourceLocation() {
        if (sourceLocation == null) {
            sourceLocation = delegate.getSourceLocation();
        }
        return sourceLocation;
    }

    @Override
    public List<String> getTags() {
        if (tags == null) {
            tags = delegate.getTags();
        }
        return tags;
    }

    @Override
    public List<String> getDependencies() {
        if (dependencies == null) {
            dependencies = delegate.getDependencies();
        }
        return dependencies;
    }

    @Override
    public String getCapabilityDescription() {
        if (capabilityDescription == null) {
            capabilityDescription = delegate.getCapabilityDescription();
        }
        return capabilityDescription;
    }

    @Override
    public String getInterface() {
        if (processorInterface == null) {
            processorInterface = delegate.getInterface();
        }
        return processorInterface;
    }

    @Override
    public List<UseCaseDetails> getUseCases() {
        if (useCases == null) {
            useCases = delegate.getUseCases();
        }
        return useCases;
    }

    @Override
    public List<MultiProcessorUseCaseDetails> getMultiProcessorUseCases() {
        if (multiProcessorUseCases == null) {
            multiProcessorUseCases = delegate.getMultiProcessorUseCases();
        }
        return multiProcessorUseCases;
    }

    @Override
    public List<PropertyDescription> getPropertyDescriptions() {
        if (propertyDescriptions == null) {
            propertyDescriptions = delegate.getPropertyDescriptions();
        }
        return propertyDescriptions;
    }

    @Override
    public void free() {
    }
}
