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
package org.apache.nifi.registry.event;

import org.apache.nifi.registry.cluster.LeaderElectionManager;
import org.apache.nifi.registry.hook.EventHookProvider;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import javax.sql.DataSource;

/**
 * Selects the active {@link EventService} implementation.
 *
 * <p>When {@code nifi.registry.cluster.enabled=false} (the default), a
 * {@link StandardEventService} is created which delivers events in-process via
 * an in-memory queue.
 *
 * <p>When {@code nifi.registry.cluster.enabled=true}, a
 * {@link ClusterAwareEventService} is created which persists events to the
 * database and delivers them exactly once from the leader node.
 */
@Configuration
public class EventServiceConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventServiceConfiguration.class);

    @Bean
    public EventService eventService(
            final List<EventHookProvider> eventHookProviders,
            final NiFiRegistryProperties properties,
            final DataSource dataSource,
            final LeaderElectionManager leaderElectionManager) {

        if (properties.isClusterEnabled()) {
            LOGGER.info("Cluster mode is enabled; creating ClusterAwareEventService.");
            return new ClusterAwareEventService(eventHookProviders, dataSource, leaderElectionManager);
        }

        LOGGER.info("Cluster mode is disabled; creating StandardEventService.");
        return new StandardEventService(eventHookProviders);
    }
}
