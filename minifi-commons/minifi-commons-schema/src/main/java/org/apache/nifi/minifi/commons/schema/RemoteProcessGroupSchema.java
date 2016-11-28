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

package org.apache.nifi.minifi.commons.schema;

import org.apache.nifi.minifi.commons.schema.common.BaseSchemaWithIdAndName;

import java.util.List;
import java.util.Map;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.COMMENT_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.INPUT_PORTS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.YIELD_PERIOD_KEY;

public class RemoteProcessGroupSchema extends BaseSchemaWithIdAndName {
    public static final String URL_KEY = "url";
    public static final String TIMEOUT_KEY = "timeout";
    public static final String TRANSPORT_PROTOCOL_KEY = "transport protocol";

    private enum transportProtocolOptions {
        RAW("RAW"), HTTP("HTTP");

        private final String stringValue;

        private transportProtocolOptions(final String s) {
            stringValue = s;
        }

        public String toString() {
            return stringValue;
        }

        public static boolean valid(String input) {
            return RAW.stringValue.equals(input) || HTTP.stringValue.equals(input);
        }
    }

    public static final String DEFAULT_COMMENT = "";
    public static final String DEFAULT_TIMEOUT = "30 secs";
    public static final String DEFAULT_YIELD_PERIOD = "10 sec";
    public static final String DEFAULT_TRANSPORT_PROTOCOL= "RAW";

    private String url;
    private List<RemoteInputPortSchema> inputPorts;

    private String comment = DEFAULT_COMMENT;
    private String timeout = DEFAULT_TIMEOUT;
    private String yieldPeriod = DEFAULT_YIELD_PERIOD;
    private String transportProtocol = DEFAULT_TRANSPORT_PROTOCOL;

    public RemoteProcessGroupSchema(Map map) {
        super(map, "RemoteProcessGroup(id: {id}, name: {name})");
        String wrapperName = getWrapperName();
        url = getRequiredKeyAsType(map, URL_KEY, String.class, wrapperName);
        inputPorts = convertListToType(getRequiredKeyAsType(map, INPUT_PORTS_KEY, List.class, wrapperName), "input port", RemoteInputPortSchema.class, INPUT_PORTS_KEY);
        if (inputPorts != null) {
            for (RemoteInputPortSchema remoteInputPortSchema: inputPorts) {
                addIssuesIfNotNull(remoteInputPortSchema);
            }
        }

        comment = getOptionalKeyAsType(map, COMMENT_KEY, String.class, wrapperName, DEFAULT_COMMENT);
        timeout = getOptionalKeyAsType(map, TIMEOUT_KEY, String.class, wrapperName, DEFAULT_TIMEOUT);
        yieldPeriod = getOptionalKeyAsType(map, YIELD_PERIOD_KEY, String.class, wrapperName, DEFAULT_YIELD_PERIOD);
        transportProtocol = getOptionalKeyAsType(map, TRANSPORT_PROTOCOL_KEY, String.class, wrapperName, DEFAULT_TRANSPORT_PROTOCOL);

        if (!transportProtocolOptions.valid(transportProtocol)){
            addValidationIssue(TRANSPORT_PROTOCOL_KEY, wrapperName, "it must be either 'RAW' or 'HTTP' but is '" + transportProtocol + "'");
        }
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = super.toMap();
        result.put(URL_KEY, url);
        result.put(COMMENT_KEY, comment);
        result.put(TIMEOUT_KEY, timeout);
        result.put(YIELD_PERIOD_KEY, yieldPeriod);
        result.put(TRANSPORT_PROTOCOL_KEY, transportProtocol);
        putListIfNotNull(result, INPUT_PORTS_KEY, inputPorts);
        return result;
    }

    public String getComment() {
        return comment;
    }

    public String getUrl() {
        return url;
    }

    public String getTimeout() {
        return timeout;
    }

    public String getYieldPeriod() {
        return yieldPeriod;
    }

    public List<RemoteInputPortSchema> getInputPorts() {
        return inputPorts;
    }

    public String getTransportProtocol() {
        return transportProtocol;
    }
}
