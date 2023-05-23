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

package org.apache.nifi.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StatelessNiFiSinkConfig extends StatelessNiFiCommonConfig {
    public static final String INPUT_PORT_NAME = "input.port";
    public static final String FAILURE_PORTS = "failure.ports";
    public static final String HEADERS_AS_ATTRIBUTES_REGEX = "headers.as.attributes.regex";
    public static final String HEADER_ATTRIBUTE_NAME_PREFIX = "attribute.prefix";
    protected static final ConfigDef CONFIG_DEF = createConfigDef();

    public StatelessNiFiSinkConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    protected StatelessNiFiSinkConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    /**
     * @return The input port name to use when feeding the flow. Can be null, which means the single available input port will be used.
     */
    public String getInputPortName() {
        return getString(INPUT_PORT_NAME);
    }

    /**
     * @return The output ports to handle as failure ports. Flow files sent to this port will cause the Connector to retry.
     */
    public Set<String> getFailurePorts() {
        final List<String> configuredPorts = getList(FAILURE_PORTS);
        if (configuredPorts == null) {
            return Collections.emptySet();
        }
        return new LinkedHashSet<>(configuredPorts);
    }

    public String getHeadersAsAttributesRegex() {
        return getString(HEADERS_AS_ATTRIBUTES_REGEX);
    }

    public String getHeaderAttributeNamePrefix() {
        return getString(HEADER_ATTRIBUTE_NAME_PREFIX);
    }

    protected static ConfigDef createConfigDef() {
        ConfigDef configDef = new ConfigDef();
        StatelessNiFiCommonConfig.addCommonConfigElements(configDef);
        addFlowConfigs(configDef);
        addSinkConfigs(configDef);
        return configDef;
    }

    /**
     * Add the flow definition related common configs to a config definition.
     *
     * @param configDef The config def to extend.
     */
    protected static void addFlowConfigs(ConfigDef configDef) {
        StatelessNiFiCommonConfig.addFlowConfigElements(configDef);
        configDef.define(INPUT_PORT_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                "The name of the Input Port to push data to", StatelessNiFiCommonConfig.FLOW_GROUP, 100,
                ConfigDef.Width.NONE, "Input port name");
        configDef.define(
                StatelessNiFiSinkConfig.FAILURE_PORTS, ConfigDef.Type.LIST, null, ConfigDef.Importance.MEDIUM,
                "A list of Output Ports that are considered failures. If any FlowFile is routed to an Output Ports whose name is provided in this property," +
                        " the session is rolled back and is considered a failure", FLOW_GROUP, 200, ConfigDef.Width.NONE, "Failure ports");
    }

    /**
     * Add sink configs to a config definition.
     *
     * @param configDef The config def to extend.
     */
    protected static void addSinkConfigs(ConfigDef configDef) {
        configDef.define(
                HEADERS_AS_ATTRIBUTES_REGEX, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                "A regular expression to evaluate against Kafka message header keys. Any message header whose key matches the regular expression" +
                        " will be added to the FlowFile as an attribute. The name of the attribute will match the header key (with an optional prefix, as " +
                        "defined by the attribute.prefix configuration) and the header value will be added as the attribute value.",
                RECORD_GROUP, 0, ConfigDef.Width.NONE, "Headers as Attributes regex");
        configDef.define(
                HEADER_ATTRIBUTE_NAME_PREFIX, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                "A prefix to add to the key of each header that matches the headers.as.attributes.regex Regular Expression. For example," +
                        " if a header has the key MyHeader and a value of MyValue, and the headers.as.attributes.regex is set to My.* and this property" +
                        " is set to kafka. then the FlowFile that is created for the Kafka message will have an attribute" +
                        " named kafka.MyHeader with a value of MyValue.",
                RECORD_GROUP, 1, ConfigDef.Width.NONE, "Headers as Attributes prefix");
    }
}
