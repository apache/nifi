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
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.nifi.kafka.connect.validators.ConnectRegularExpressionValidator;
import org.apache.nifi.stateless.flow.StatelessDataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatelessNiFiSourceConnector extends SourceConnector {
    static final String OUTPUT_PORT_NAME = "output.port";
    static final String TOPIC_NAME = "topics";

    static final String TOPIC_NAME_ATTRIBUTE = "topic.name.attribute";
    static final String KEY_ATTRIBUTE = "key.attribute";
    static final String HEADER_REGEX = "header.attribute.regex";

    private Map<String, String> properties;
    private boolean primaryNodeOnly;

    @Override
    public void start(final Map<String, String> properties) {
        this.properties = new HashMap<>(properties);

        final StatelessDataflow dataflow = StatelessKafkaConnectorUtil.createDataflow(properties);
        primaryNodeOnly = dataflow.isSourcePrimaryNodeOnly();
        dataflow.shutdown();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return StatelessNiFiSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final int numTasks = primaryNodeOnly ? 1 : maxTasks;

        final List<Map<String, String>> configs = new ArrayList<>();
        for (int i=0; i < numTasks; i++) {
            final Map<String, String> taskConfig = new HashMap<>(properties);
            taskConfig.put("task.index", String.valueOf(i));
            configs.add(taskConfig);
        }

        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        final ConfigDef configDef = new ConfigDef();
        StatelessKafkaConnectorUtil.addCommonConfigElements(configDef);

        configDef.define(OUTPUT_PORT_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "The name of the Output Port to pull data from");
        configDef.define(TOPIC_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "The name of the Kafka topic to send data to. Either the topics or topic.name.attribute configuration must be specified.");

        configDef.define(TOPIC_NAME_ATTRIBUTE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
            "Specifies the name of a FlowFile attribute to use for determining which Kafka Topic a FlowFile"
                + " will be sent to. Either the " + TOPIC_NAME + " or " + TOPIC_NAME_ATTRIBUTE + " configuration must be specified. If both are specified, the " + TOPIC_NAME_ATTRIBUTE
                + " will be preferred, but if a FlowFile does not have the specified attribute name, then the " + TOPIC_NAME + " property will serve as the default topic name to use.");

        configDef.define(KEY_ATTRIBUTE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Specifies the name of a FlowFile attribute to use for determining the Kafka Message key. If not"
            + " specified, the message key will be null. If specified, the value of the attribute with the given name will be used as the message key.");

        configDef.define(HEADER_REGEX, ConfigDef.Type.STRING, null, new ConnectRegularExpressionValidator(), ConfigDef.Importance.MEDIUM,
            "Specifies a Regular Expression to evaluate against all FlowFile attributes. Any attribute whose name"
            + " matches the Regular Expression will be converted into a Kafka message header with the name of the attribute used as header key and the value of the attribute used as the header"
            + " value.");

        return configDef;
    }

    @Override
    public String version() {
        return StatelessKafkaConnectorUtil.getVersion();
    }
}
