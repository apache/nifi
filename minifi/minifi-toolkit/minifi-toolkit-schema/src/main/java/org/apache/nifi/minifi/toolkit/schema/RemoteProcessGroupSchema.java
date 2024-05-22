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

package org.apache.nifi.minifi.toolkit.schema;

import org.apache.nifi.minifi.toolkit.schema.common.BaseSchemaWithIdAndName;
import org.apache.nifi.minifi.toolkit.schema.common.StringUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.COMMENT_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.INPUT_PORTS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.OUTPUT_PORTS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.YIELD_PERIOD_KEY;

public class RemoteProcessGroupSchema extends BaseSchemaWithIdAndName {
    public static final String URL_KEY = "url";
    public static final String TIMEOUT_KEY = "timeout";
    public static final String TRANSPORT_PROTOCOL_KEY = "transport protocol";
    public static final String S2S_PROXY_REQUIRES_HTTP = "Site-To-Site proxy support requires HTTP " + TRANSPORT_PROTOCOL_KEY;
    public static final String PROXY_HOST_KEY = "proxy host";
    public static final String PROXY_PORT_KEY = "proxy port";
    public static final String PROXY_USER_KEY = "proxy user";
    public static final String PROXY_PASSWORD_KEY = "proxy password";
    public static final String LOCAL_NETWORK_INTERFACE_KEY = "local network interface";

    public static final String EXPECTED_PROXY_HOST_IF_PROXY_PORT = "expected " + PROXY_HOST_KEY + " to be set if " + PROXY_PORT_KEY + " is";
    public static final String EXPECTED_PROXY_HOST_IF_PROXY_USER = "expected " + PROXY_HOST_KEY + " to be set if " + PROXY_USER_KEY + " is";
    public static final String EXPECTED_PROXY_USER_IF_PROXY_PASSWORD = "expected " + PROXY_USER_KEY + " to be set if " + PROXY_PASSWORD_KEY + " is";
    public static final String EXPECTED_PROXY_PASSWORD_IF_PROXY_USER = "expected " + PROXY_PASSWORD_KEY + " to be set if " + PROXY_USER_KEY + " is";

    public enum TransportProtocolOptions {
        RAW, HTTP;

        public static boolean valid(String input) {
            return RAW.name().equals(input) || HTTP.name().equals(input);
        }
    }

    public static final String DEFAULT_COMMENT = "";
    public static final String DEFAULT_TIMEOUT = "30 secs";
    public static final String DEFAULT_YIELD_PERIOD = "10 sec";
    public static final String DEFAULT_TRANSPORT_PROTOCOL = "RAW";
    public static final String DEFAULT_PROXY_HOST = "";
    public static final Integer DEFAULT_PROXY_PORT = null;
    public static final String DEFAULT_PROXY_USER = "";
    public static final String DEFAULT_PROXY_PASSWORD = "";
    public static final String DEFAULT_NETWORK_INTERFACE = "";

    private final String urls;
    private List<RemotePortSchema> inputPorts;
    private List<RemotePortSchema> outputPorts;

    private String comment = DEFAULT_COMMENT;
    private String timeout = DEFAULT_TIMEOUT;
    private String yieldPeriod = DEFAULT_YIELD_PERIOD;
    private String transportProtocol = DEFAULT_TRANSPORT_PROTOCOL;
    private String proxyHost = DEFAULT_PROXY_HOST;
    private Integer proxyPort = DEFAULT_PROXY_PORT;
    private String proxyUser = DEFAULT_PROXY_USER;
    private String proxyPassword = DEFAULT_PROXY_PASSWORD;
    private String localNetworkInterface = DEFAULT_NETWORK_INTERFACE;

    public RemoteProcessGroupSchema(Map map) {
        super(map, "RemoteProcessGroup(id: {id}, name: {name})");
        String wrapperName = getWrapperName();
        // This is either a singular URL or a comma separated list
        urls = getRequiredKeyAsType(map, URL_KEY, String.class, wrapperName);


        inputPorts = convertListToType(getOptionalKeyAsType(map, INPUT_PORTS_KEY, List.class, wrapperName, new ArrayList<>()), "input port", RemotePortSchema.class, INPUT_PORTS_KEY);
        addIssuesIfNotNull(inputPorts);

        outputPorts = convertListToType(getOptionalKeyAsType(map, OUTPUT_PORTS_KEY, List.class, wrapperName, new ArrayList<>()), "output port", RemotePortSchema.class, OUTPUT_PORTS_KEY);
        addIssuesIfNotNull(outputPorts);

        if (inputPorts.size() == 0 && outputPorts.size() == 0) {
            addValidationIssue("Expected either '" + INPUT_PORTS_KEY + "', '" + OUTPUT_PORTS_KEY + "' in section '" + wrapperName + "' to have value(s)");
        }

        comment = getOptionalKeyAsType(map, COMMENT_KEY, String.class, wrapperName, DEFAULT_COMMENT);
        timeout = getOptionalKeyAsType(map, TIMEOUT_KEY, String.class, wrapperName, DEFAULT_TIMEOUT);
        yieldPeriod = getOptionalKeyAsType(map, YIELD_PERIOD_KEY, String.class, wrapperName, DEFAULT_YIELD_PERIOD);
        transportProtocol = getOptionalKeyAsType(map, TRANSPORT_PROTOCOL_KEY, String.class, wrapperName, DEFAULT_TRANSPORT_PROTOCOL);

        if (!TransportProtocolOptions.valid(transportProtocol)) {
            addValidationIssue(TRANSPORT_PROTOCOL_KEY, wrapperName, "it must be either 'RAW' or 'HTTP' but is '" + transportProtocol + "'");
        }

        localNetworkInterface = getOptionalKeyAsType(map, LOCAL_NETWORK_INTERFACE_KEY, String.class, wrapperName, DEFAULT_NETWORK_INTERFACE);

        proxyHost = getOptionalKeyAsType(map, PROXY_HOST_KEY, String.class, wrapperName, DEFAULT_PROXY_HOST);
        proxyPort = getOptionalKeyAsType(map, PROXY_PORT_KEY, Integer.class, wrapperName, DEFAULT_PROXY_PORT);
        proxyUser = getOptionalKeyAsType(map, PROXY_USER_KEY, String.class, wrapperName, DEFAULT_PROXY_USER);
        proxyPassword = getOptionalKeyAsType(map, PROXY_PASSWORD_KEY, String.class, wrapperName, DEFAULT_PROXY_PASSWORD);

        if (StringUtil.isNullOrEmpty(proxyHost)) {
            if (proxyPort != null) {
                addValidationIssue(PROXY_PORT_KEY, wrapperName, EXPECTED_PROXY_HOST_IF_PROXY_PORT);
            }
            if (!StringUtil.isNullOrEmpty(proxyUser)) {
                addValidationIssue(PROXY_USER_KEY, wrapperName, EXPECTED_PROXY_HOST_IF_PROXY_USER);
            }
        } else if (!TransportProtocolOptions.HTTP.name().equals(transportProtocol)) {
            addValidationIssue(PROXY_HOST_KEY, wrapperName, S2S_PROXY_REQUIRES_HTTP);
        }

        if (StringUtil.isNullOrEmpty(proxyUser)) {
            if (!StringUtil.isNullOrEmpty(proxyPassword)) {
                addValidationIssue(PROXY_PASSWORD_KEY, wrapperName, EXPECTED_PROXY_USER_IF_PROXY_PASSWORD);
            }
        } else if (StringUtil.isNullOrEmpty(proxyPassword)) {
            addValidationIssue(PROXY_USER_KEY, wrapperName, EXPECTED_PROXY_PASSWORD_IF_PROXY_USER);
        }

    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = super.toMap();
        result.put(URL_KEY, urls);
        result.put(COMMENT_KEY, comment);
        result.put(TIMEOUT_KEY, timeout);
        result.put(YIELD_PERIOD_KEY, yieldPeriod);
        result.put(TRANSPORT_PROTOCOL_KEY, transportProtocol);
        result.put(PROXY_HOST_KEY, proxyHost);
        result.put(PROXY_PORT_KEY, proxyPort == null ? "" : proxyPort);
        result.put(PROXY_USER_KEY, proxyUser);
        result.put(PROXY_PASSWORD_KEY, proxyPassword);
        result.put(LOCAL_NETWORK_INTERFACE_KEY, localNetworkInterface);
        putListIfNotNull(result, INPUT_PORTS_KEY, inputPorts);
        putListIfNotNull(result, OUTPUT_PORTS_KEY, outputPorts);
        return result;
    }

    public String getComment() {
        return comment;
    }

    public String getUrls() {
        return urls;
    }

    public String getTimeout() {
        return timeout;
    }

    public String getYieldPeriod() {
        return yieldPeriod;
    }

    public List<RemotePortSchema> getInputPorts() {
        return inputPorts;
    }

    public List<RemotePortSchema> getOutputPorts() {
        return outputPorts;
    }

    public String getTransportProtocol() {
        return transportProtocol;
    }

    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    public void setProxyPort(Integer proxyPort) {
        this.proxyPort = proxyPort;
    }

    public void setProxyUser(String proxyUser) {
        this.proxyUser = proxyUser;
    }

    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    public void setTransportProtocol(String transportProtocol) {
        this.transportProtocol = transportProtocol;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public Integer getProxyPort() {
        return proxyPort;
    }

    public String getProxyUser() {
        return proxyUser;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }

    public void setLocalNetworkInterface(String LocalNetworkInterface) {
        this.localNetworkInterface = LocalNetworkInterface;
    }

    public String getLocalNetworkInterface() {
        return localNetworkInterface;
    }
}
