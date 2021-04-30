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

package org.apache.nifi.minifi.commons.schema.v2;

import org.apache.nifi.minifi.commons.schema.RemotePortSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema.TransportProtocolOptions;
import org.apache.nifi.minifi.commons.schema.common.BaseSchema;
import org.apache.nifi.minifi.commons.schema.common.BaseSchemaWithIdAndName;
import org.apache.nifi.minifi.commons.schema.common.ConvertableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema.DEFAULT_COMMENT;
import static org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema.DEFAULT_TIMEOUT;
import static org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema.DEFAULT_TRANSPORT_PROTOCOL;
import static org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema.DEFAULT_YIELD_PERIOD;
import static org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema.TIMEOUT_KEY;
import static org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema.TRANSPORT_PROTOCOL_KEY;
import static org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema.URL_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.COMMENT_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.INPUT_PORTS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.YIELD_PERIOD_KEY;

public class RemoteProcessGroupSchemaV2 extends BaseSchema implements ConvertableSchema<RemoteProcessGroupSchema> {
    private BaseSchemaWithIdAndName idAndName;
    private String url;
    private List<RemotePortSchema> inputPorts;

    private String comment = DEFAULT_COMMENT;
    private String timeout = DEFAULT_TIMEOUT;
    private String yieldPeriod = DEFAULT_YIELD_PERIOD;
    private String transportProtocol = DEFAULT_TRANSPORT_PROTOCOL;

    public RemoteProcessGroupSchemaV2(Map map) {
        idAndName = new BaseSchemaWithIdAndName(map, "RemoteProcessGroup(id: {id}, name: {name})");

        String wrapperName = idAndName.getWrapperName();
        url = getRequiredKeyAsType(map, URL_KEY, String.class, wrapperName);
        inputPorts = convertListToType(getRequiredKeyAsType(map, INPUT_PORTS_KEY, List.class, wrapperName), "input port", RemotePortSchema.class, INPUT_PORTS_KEY);
        if (inputPorts != null) {
            for (RemotePortSchema remoteInputPortSchema: inputPorts) {
                addIssuesIfNotNull(remoteInputPortSchema);
            }
        }

        comment = getOptionalKeyAsType(map, COMMENT_KEY, String.class, wrapperName, DEFAULT_COMMENT);
        timeout = getOptionalKeyAsType(map, TIMEOUT_KEY, String.class, wrapperName, DEFAULT_TIMEOUT);
        yieldPeriod = getOptionalKeyAsType(map, YIELD_PERIOD_KEY, String.class, wrapperName, DEFAULT_YIELD_PERIOD);
        transportProtocol = getOptionalKeyAsType(map, TRANSPORT_PROTOCOL_KEY, String.class, wrapperName, DEFAULT_TRANSPORT_PROTOCOL);

        if (!TransportProtocolOptions.valid(transportProtocol)){
            addValidationIssue(TRANSPORT_PROTOCOL_KEY, wrapperName, "it must be either 'RAW' or 'HTTP' but is '" + transportProtocol + "'");
        }
    }

    @Override
    public RemoteProcessGroupSchema convert() {
        Map<String, Object> result = idAndName.toMap();
        result.put(URL_KEY, url);
        result.put(COMMENT_KEY, comment);
        result.put(TIMEOUT_KEY, timeout);
        result.put(YIELD_PERIOD_KEY, yieldPeriod);
        result.put(TRANSPORT_PROTOCOL_KEY, transportProtocol);
        putListIfNotNull(result, INPUT_PORTS_KEY, inputPorts);
        return new RemoteProcessGroupSchema(result);
    }

    @Override
    public List<String> getValidationIssues() {
        List<String> validationIssues = new ArrayList<>(idAndName.getValidationIssues());
        validationIssues.addAll(super.getValidationIssues());
        return validationIssues;
    }

    @Override
    public int getVersion() {
        return ConfigSchemaV2.CONFIG_VERSION;
    }

    public String getTransportProtocol() {
        return transportProtocol;
    }

    public String getName() {
        return idAndName.getName();
    }

    public String getId() {
        return idAndName.getId();
    }

    public List<RemotePortSchema> getInputPorts() {
        return inputPorts;
    }
}
