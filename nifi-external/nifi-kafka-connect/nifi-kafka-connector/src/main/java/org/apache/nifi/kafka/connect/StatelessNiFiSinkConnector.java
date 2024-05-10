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

/**
 * NiFi Kafka Connect is Deprecated and will be removed in a later release
 */
@Deprecated
public class StatelessNiFiSinkConnector extends SinkConnector {

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
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new HashMap<>(properties));
        }

        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return StatelessNiFiSinkConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return StatelessKafkaConnectorUtil.getVersion();
    }
}
