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
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatelessNiFiSinkConnector extends SinkConnector {
    static final String INPUT_PORT_NAME = "input.port";
    static final String FAILURE_PORTS = "failure.ports";
    static final String HEADERS_AS_ATTRIBUTES_REGEX = "headers.as.attributes.regex";
    static final String HEADER_ATTRIBUTE_NAME_PREFIX = "attribute.prefix";

    private Map<String, String> properties;

    @Override
    public void start(final Map<String, String> properties) {
        this.properties = new HashMap<>(properties);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return StatelessNiFiSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final List<Map<String, String>> configs = new ArrayList<>();
        for (int i=0; i < maxTasks; i++) {
            configs.add(new HashMap<>(properties));
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

        configDef.define(INPUT_PORT_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "The name of the Input Port to push data to");
        configDef.define(HEADERS_AS_ATTRIBUTES_REGEX, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
            "A regular expression to evaluate against Kafka message header keys. Any message " +
            "header whose key matches the regular expression will be added to the FlowFile as an attribute. The name of the attribute will match the header key (with an optional prefix, as " +
            "defined by the attribute.prefix configuration) and the header value will be added as the attribute value.");
        configDef.define(HEADER_ATTRIBUTE_NAME_PREFIX, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
            "A prefix to add to the key of each header that matches the headers.as.attributes.regex Regular Expression. For example, if a header has the key MyHeader and a value of " +
                "MyValue, and the headers.as.attributes.regex is set to My.* and this property is set to kafka. then the FlowFile that is created for the Kafka message will have an attribute" +
                " named kafka.MyHeader with a value of MyValue.");

        configDef.define(FAILURE_PORTS, ConfigDef.Type.LIST, null, ConfigDef.Importance.MEDIUM,
            "A list of Output Ports that are considered failures. If any FlowFile is routed to an Output Ports whose name is provided in this property, the session is rolled back and is considered " +
                "a failure");

        return configDef;
    }

    @Override
    public String version() {
        return StatelessKafkaConnectorUtil.getVersion();
    }
}
