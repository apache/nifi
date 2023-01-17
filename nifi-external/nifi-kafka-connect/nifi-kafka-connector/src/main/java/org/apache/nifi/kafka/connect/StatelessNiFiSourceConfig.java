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
import org.apache.nifi.kafka.connect.validators.ConnectRegularExpressionValidator;

import java.util.Map;

public class StatelessNiFiSourceConfig extends StatelessNiFiCommonConfig {
    public static final String OUTPUT_PORT_NAME = "output.port";
    public static final String TOPIC_NAME = "topics";
    public static final String TOPIC_NAME_ATTRIBUTE = "topic.name.attribute";
    public static final String KEY_ATTRIBUTE = "key.attribute";
    public static final String HEADER_REGEX = "header.attribute.regex";
    public static final String STATE_MAP_KEY = "task.index";
    protected static final ConfigDef CONFIG_DEF = createConfigDef();

    public StatelessNiFiSourceConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    protected StatelessNiFiSourceConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    /**
     * @return The output port name to use when reading from the flow. Can be null, which means the single available output port will be used.
     */
    public String getOutputPortName() {
        return getString(OUTPUT_PORT_NAME);
    }

    public String getTopicName() {
        return getString(TOPIC_NAME);
    }

    public String getTopicNameAttribute() {
        return getString(TOPIC_NAME_ATTRIBUTE);
    }

    public String getKeyAttribute() {
        return getString(KEY_ATTRIBUTE);
    }

    public String getHeaderRegex() {
        return getString(HEADER_REGEX);
    }

    public String getStateMapKey() {
        return originalsStrings().get(STATE_MAP_KEY);
    }

    protected static ConfigDef createConfigDef() {
        ConfigDef configDef = new ConfigDef();
        StatelessNiFiCommonConfig.addCommonConfigElements(configDef);
        addFlowConfigs(configDef);
        addSourceConfigs(configDef);
        return configDef;
    }

    /**
     * Add the flow definition related common configs to a config definition.
     *
     * @param configDef The config def to extend.
     */
    protected static void addFlowConfigs(ConfigDef configDef) {
        StatelessNiFiCommonConfig.addFlowConfigElements(configDef);
        configDef.define(StatelessNiFiSourceConfig.OUTPUT_PORT_NAME, ConfigDef.Type.STRING, null,
                ConfigDef.Importance.HIGH, "The name of the Output Port to pull data from",
                FLOW_GROUP, 100, ConfigDef.Width.NONE, "Output port name");
    }

    /**
     * Add sink configs to a config definition.
     *
     * @param configDef The config def to extend.
     */
    protected static void addSourceConfigs(ConfigDef configDef) {
        configDef.define(
                StatelessNiFiSourceConfig.TOPIC_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                "The name of the Kafka topic to send data to. Either the topics or topic.name.attribute configuration must be specified.",
                RECORD_GROUP, 0, ConfigDef.Width.NONE, "Topic name");
        configDef.define(
                StatelessNiFiSourceConfig.TOPIC_NAME_ATTRIBUTE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                "Specifies the name of a FlowFile attribute to use for determining which Kafka Topic a FlowFile"
                        + " will be sent to. Either the " + StatelessNiFiSourceConfig.TOPIC_NAME + " or " + StatelessNiFiSourceConfig.TOPIC_NAME_ATTRIBUTE +
                        " configuration must be specified. If both are specified, the " + StatelessNiFiSourceConfig.TOPIC_NAME_ATTRIBUTE
                        + " will be preferred, but if a FlowFile does not have the specified attribute name, then the " + StatelessNiFiSourceConfig.TOPIC_NAME +
                        " property will serve as the default topic name to use.",
                RECORD_GROUP, 1, ConfigDef.Width.NONE, "Topic name attribute");

        configDef.define(StatelessNiFiSourceConfig.KEY_ATTRIBUTE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                "Specifies the name of a FlowFile attribute to use for determining the Kafka Message key. If not"
                        + " specified, the message key will be null. If specified, the value of the attribute with the given name will be used as the message key.",
                RECORD_GROUP, 100, ConfigDef.Width.NONE, "Record key attribute");

        configDef.define(
                StatelessNiFiSourceConfig.HEADER_REGEX, ConfigDef.Type.STRING, null, new ConnectRegularExpressionValidator(), ConfigDef.Importance.MEDIUM,
                "Specifies a Regular Expression to evaluate against all FlowFile attributes. Any attribute whose name matches the Regular Expression" +
                        " will be converted into a Kafka message header with the name of the attribute used as header key and the value of the attribute used as the header"
                        + " value.",
                RECORD_GROUP, 200, ConfigDef.Width.NONE, "Record header attribute regex");
    }
}
