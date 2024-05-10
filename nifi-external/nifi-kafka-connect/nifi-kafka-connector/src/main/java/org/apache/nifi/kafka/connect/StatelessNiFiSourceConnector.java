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
import org.apache.nifi.stateless.flow.StatelessDataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * NiFi Kafka Connect is Deprecated and will be removed in a later release
 */
@Deprecated
public class StatelessNiFiSourceConnector extends SourceConnector {

    private StatelessNiFiSourceConfig config;
    private boolean primaryNodeOnly;

    @Override
    public void start(final Map<String, String> properties) {
        config = createConfig(properties);

        final StatelessDataflow dataflow = StatelessKafkaConnectorUtil.createDataflow(config);
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
        for (int i = 0; i < numTasks; i++) {
            final Map<String, String> taskConfig = new HashMap<>(config.originalsStrings());
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
        return StatelessNiFiSourceConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return StatelessKafkaConnectorUtil.getVersion();
    }

    /**
     * Creates a config instance to be used by the Connector.
     *
     * @param properties Properties to use in the config.
     * @return The config instance.
     */
    protected StatelessNiFiSourceConfig createConfig(Map<String, String> properties) {
        return new StatelessNiFiSourceConfig(properties);
    }
}
