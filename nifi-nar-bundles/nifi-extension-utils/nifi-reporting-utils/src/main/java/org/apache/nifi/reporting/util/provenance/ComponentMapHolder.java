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
package org.apache.nifi.reporting.util.provenance;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;

import java.util.HashMap;
import java.util.Map;

public class ComponentMapHolder {
    final Map<String,String> componentMap = new HashMap<>();
    final Map<String,String> componentToParentGroupMap = new HashMap<>();

    public ComponentMapHolder putAll(ComponentMapHolder holder) {
        this.componentMap.putAll(holder.getComponentMap());
        this.componentToParentGroupMap.putAll(holder.getComponentToParentGroupMap());
        return this;
    }

    public Map<String, String> getComponentMap() {
        return componentMap;
    }

    public Map<String, String> getComponentToParentGroupMap() {
        return componentToParentGroupMap;
    }

    public String getComponentName(final String componentId) {
        return componentMap.get(componentId);
    }

    public String getProcessGroupId(final String componentId) {
        return componentToParentGroupMap.get(componentId);
    }

    public static ComponentMapHolder createComponentMap(final ProcessGroupStatus status) {
        final ComponentMapHolder holder = new ComponentMapHolder();
        final Map<String,String> componentMap = holder.getComponentMap();
        final Map<String,String> componentToParentGroupMap = holder.getComponentToParentGroupMap();

        if (status != null) {
            componentMap.put(status.getId(), status.getName());

            for (final ProcessorStatus procStatus : status.getProcessorStatus()) {
                componentMap.put(procStatus.getId(), procStatus.getName());
                componentToParentGroupMap.put(procStatus.getId(), status.getId());
            }

            for (final PortStatus portStatus : status.getInputPortStatus()) {
                componentMap.put(portStatus.getId(), portStatus.getName());
                componentToParentGroupMap.put(portStatus.getId(), status.getId());
            }

            for (final PortStatus portStatus : status.getOutputPortStatus()) {
                componentMap.put(portStatus.getId(), portStatus.getName());
                componentToParentGroupMap.put(portStatus.getId(), status.getId());
            }

            for (final RemoteProcessGroupStatus rpgStatus : status.getRemoteProcessGroupStatus()) {
                componentMap.put(rpgStatus.getId(), rpgStatus.getName());
                componentToParentGroupMap.put(rpgStatus.getId(), status.getId());
            }

            for (final ConnectionStatus connectionStatus : status.getConnectionStatus()) {
                componentMap.put(connectionStatus.getId(), connectionStatus.getName());
                componentToParentGroupMap.put(connectionStatus.getId(), status.getId());
            }

            for (final ProcessGroupStatus childGroup : status.getProcessGroupStatus()) {
                componentMap.put(childGroup.getId(), childGroup.getName());
                componentToParentGroupMap.put(childGroup.getId(), status.getId());
                holder.putAll(createComponentMap(childGroup));
            }
        }

        return holder;
    }

}
