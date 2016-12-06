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

package org.apache.nifi.minifi.toolkit.configuration.dto;

import org.apache.nifi.minifi.commons.schema.RemoteInputPortSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RemoteProcessGroupSchemaFunction implements Function<RemoteProcessGroupDTO, RemoteProcessGroupSchema> {
    private final RemoteInputPortSchemaFunction remoteInputPortSchemaFunction;

    public RemoteProcessGroupSchemaFunction(RemoteInputPortSchemaFunction remoteInputPortSchemaFunction) {
        this.remoteInputPortSchemaFunction = remoteInputPortSchemaFunction;
    }

    @Override
    public RemoteProcessGroupSchema apply(RemoteProcessGroupDTO remoteProcessGroupDTO) {
        Map<String, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.ID_KEY, remoteProcessGroupDTO.getId());
        map.put(CommonPropertyKeys.NAME_KEY, remoteProcessGroupDTO.getName());
        map.put(RemoteProcessGroupSchema.URL_KEY, remoteProcessGroupDTO.getTargetUri());

        RemoteProcessGroupContentsDTO contents = remoteProcessGroupDTO.getContents();
        if (contents != null) {
            Set<RemoteProcessGroupPortDTO> inputPorts = contents.getInputPorts();
            if (inputPorts != null) {
                map.put(CommonPropertyKeys.INPUT_PORTS_KEY, inputPorts.stream()
                        .map(remoteInputPortSchemaFunction)
                        .map(RemoteInputPortSchema::toMap)
                        .collect(Collectors.toList()));
            }
        }

        map.put(CommonPropertyKeys.COMMENT_KEY, remoteProcessGroupDTO.getComments());
        map.put(RemoteProcessGroupSchema.TIMEOUT_KEY, remoteProcessGroupDTO.getCommunicationsTimeout());
        map.put(CommonPropertyKeys.YIELD_PERIOD_KEY, remoteProcessGroupDTO.getYieldDuration());
        map.put(RemoteProcessGroupSchema.TRANSPORT_PROTOCOL_KEY, remoteProcessGroupDTO.getTransportProtocol());
        map.put(RemoteProcessGroupSchema.PROXY_HOST_KEY, remoteProcessGroupDTO.getProxyHost());
        map.put(RemoteProcessGroupSchema.PROXY_PORT_KEY, remoteProcessGroupDTO.getProxyPort());
        map.put(RemoteProcessGroupSchema.PROXY_USER_KEY, remoteProcessGroupDTO.getProxyUser());
        map.put(RemoteProcessGroupSchema.PROXY_PASSWORD_KEY, remoteProcessGroupDTO.getProxyPassword());
        return new RemoteProcessGroupSchema(map);
    }
}
