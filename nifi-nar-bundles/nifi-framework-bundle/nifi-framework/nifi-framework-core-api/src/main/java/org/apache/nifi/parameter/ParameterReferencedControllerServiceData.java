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
package org.apache.nifi.parameter;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ControllerService;

public class ParameterReferencedControllerServiceData {
    private final String parameterName;
    private final ComponentNode componentNode;
    private final PropertyDescriptor descriptor;
    private final Class<? extends ControllerService> referencedControllerServiceType;
    private final String versionedServiceId;

    public ParameterReferencedControllerServiceData(
        String parameterName,
        ComponentNode componentNode,
        PropertyDescriptor descriptor,
        Class<? extends ControllerService> referencedControllerServiceType,
        String versionedServiceId
    ) {
        this.parameterName = parameterName;
        this.componentNode = componentNode;
        this.descriptor = descriptor;
        this.referencedControllerServiceType = referencedControllerServiceType;
        this.versionedServiceId = versionedServiceId;
    }

    public String getParameterName() {
        return parameterName;
    }

    public ComponentNode getComponentNode() {
        return componentNode;
    }

    public PropertyDescriptor getDescriptor() {
        return descriptor;
    }

    public Class<? extends ControllerService> getReferencedControllerServiceType() {
        return referencedControllerServiceType;
    }

    public String getVersionedServiceId() {
        return versionedServiceId;
    }
}
