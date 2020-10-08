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

package org.apache.nifi.registry.flow.mapping;

import org.apache.nifi.registry.flow.ExternalControllerServiceReference;
import org.apache.nifi.registry.flow.VersionedProcessGroup;

import javax.xml.bind.annotation.XmlTransient;
import java.util.Map;

public class InstantiatedVersionedProcessGroup extends VersionedProcessGroup implements InstantiatedVersionedComponent {

    private final String instanceId;
    private final String groupId;
    private Map<String, ExternalControllerServiceReference> externalControllerServiceReferences;

    public InstantiatedVersionedProcessGroup(final String instanceId, final String instanceGroupId) {
        this.instanceId = instanceId;
        this.groupId = instanceGroupId;
    }

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public String getInstanceGroupId() {
        return groupId;
    }

    public void setExternalControllerServiceReferences(final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences) {
        this.externalControllerServiceReferences = externalControllerServiceReferences;
    }

    // mark transient so field is ignored when serializing this class
    @XmlTransient
    public Map<String, ExternalControllerServiceReference> getExternalControllerServiceReferences() {
        return externalControllerServiceReferences;
    }
}
