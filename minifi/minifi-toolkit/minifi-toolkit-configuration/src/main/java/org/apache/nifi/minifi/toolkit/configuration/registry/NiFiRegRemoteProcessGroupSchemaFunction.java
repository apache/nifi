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
package org.apache.nifi.minifi.toolkit.configuration.registry;

import org.apache.nifi.minifi.commons.schema.RemotePortSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.registry.flow.VersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.VersionedRemoteProcessGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class NiFiRegRemoteProcessGroupSchemaFunction implements Function<VersionedRemoteProcessGroup, RemoteProcessGroupSchema> {

    private final NiFiRegRemotePortSchemaFunction remotePortSchemaFunction;

    public NiFiRegRemoteProcessGroupSchemaFunction(NiFiRegRemotePortSchemaFunction remotePortSchemaFunction) {
        this.remotePortSchemaFunction = remotePortSchemaFunction;
    }
    @Override
    public RemoteProcessGroupSchema apply(VersionedRemoteProcessGroup versionedRemoteProcessGroup) {
        Map<String, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.ID_KEY, versionedRemoteProcessGroup.getIdentifier());
        map.put(CommonPropertyKeys.NAME_KEY, versionedRemoteProcessGroup.getName());
        map.put(RemoteProcessGroupSchema.URL_KEY, versionedRemoteProcessGroup.getTargetUri());

        Set<VersionedRemoteGroupPort> inputPorts = versionedRemoteProcessGroup.getInputPorts();
        if (inputPorts != null) {
            map.put(CommonPropertyKeys.INPUT_PORTS_KEY, inputPorts.stream()
                    .map(remotePortSchemaFunction)
                    .map(RemotePortSchema::toMap)
                    .collect(Collectors.toList()));
        }

        Set<VersionedRemoteGroupPort> outputPorts = versionedRemoteProcessGroup.getOutputPorts();
        if (outputPorts != null) {
            map.put(CommonPropertyKeys.OUTPUT_PORTS_KEY, outputPorts.stream()
                    .map(remotePortSchemaFunction)
                    .map(RemotePortSchema::toMap)
                    .collect(Collectors.toList()));
        }


        map.put(CommonPropertyKeys.COMMENT_KEY, versionedRemoteProcessGroup.getComments());
        map.put(RemoteProcessGroupSchema.TIMEOUT_KEY, versionedRemoteProcessGroup.getCommunicationsTimeout());
        map.put(CommonPropertyKeys.YIELD_PERIOD_KEY, versionedRemoteProcessGroup.getYieldDuration());
        map.put(RemoteProcessGroupSchema.TRANSPORT_PROTOCOL_KEY, versionedRemoteProcessGroup.getTransportProtocol());
        map.put(RemoteProcessGroupSchema.PROXY_HOST_KEY, versionedRemoteProcessGroup.getProxyHost());
        map.put(RemoteProcessGroupSchema.PROXY_PORT_KEY, versionedRemoteProcessGroup.getProxyPort());
        map.put(RemoteProcessGroupSchema.PROXY_USER_KEY, versionedRemoteProcessGroup.getProxyUser());

        // TODO - we don't have this in registry data model, most likely templates blank it out too?
        //map.put(RemoteProcessGroupSchema.PROXY_PASSWORD_KEY, versionedRemoteProcessGroup.getProxyPassword());

        map.put(RemoteProcessGroupSchema.LOCAL_NETWORK_INTERFACE_KEY, versionedRemoteProcessGroup.getLocalNetworkInterface());
        return new RemoteProcessGroupSchema(map);
    }
}
