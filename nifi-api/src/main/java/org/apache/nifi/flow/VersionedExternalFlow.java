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

package org.apache.nifi.flow;

import java.util.Map;

public class VersionedExternalFlow {
    private VersionedProcessGroup flowContents;
    private Map<String, ExternalControllerServiceReference> externalControllerServices;
    private Map<String, VersionedParameterContext> parameterContexts;
    private Map<String, ParameterProviderReference> parameterProviders;
    private VersionedExternalFlowMetadata metadata;

    public VersionedProcessGroup getFlowContents() {
        return flowContents;
    }

    public void setFlowContents(final VersionedProcessGroup flowContents) {
        this.flowContents = flowContents;
    }

    public Map<String, ExternalControllerServiceReference> getExternalControllerServices() {
        return externalControllerServices;
    }

    public void setExternalControllerServices(final Map<String, ExternalControllerServiceReference> externalControllerServices) {
        this.externalControllerServices = externalControllerServices;
    }

    public Map<String, VersionedParameterContext> getParameterContexts() {
        return parameterContexts;
    }

    public void setParameterContexts(final Map<String, VersionedParameterContext> parameterContexts) {
        this.parameterContexts = parameterContexts;
    }

    public VersionedExternalFlowMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(final VersionedExternalFlowMetadata metadata) {
        this.metadata = metadata;
    }

    public Map<String, ParameterProviderReference> getParameterProviders() {
        return parameterProviders;
    }

    public void setParameterProviders(final Map<String, ParameterProviderReference> parameterProviders) {
        this.parameterProviders = parameterProviders;
    }
}
