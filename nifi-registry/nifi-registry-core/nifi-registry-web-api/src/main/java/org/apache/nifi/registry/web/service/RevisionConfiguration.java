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
package org.apache.nifi.registry.web.service;

import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.revision.api.RevisionManager;
import org.apache.nifi.registry.revision.entity.RevisableEntityService;
import org.apache.nifi.registry.revision.entity.StandardRevisableEntityService;
import org.apache.nifi.registry.revision.jdbc.JdbcRevisionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Creates the beans needed for revision management.
 */
@Configuration
public class RevisionConfiguration {

    @Bean
    public synchronized RevisionManager getRevisionManager(final JdbcTemplate jdbcTemplate) {
        return new JdbcRevisionManager(jdbcTemplate);
    }

    @Bean
    public synchronized RevisableEntityService getRevisableEntityService(final RevisionManager revisionManager) {
        return new StandardRevisableEntityService(revisionManager);
    }

    @Bean
    public synchronized RevisionFeature getRevisionFeature(final NiFiRegistryProperties properties) {
        return () -> {
            return properties.areRevisionsEnabled();
        };
    }

}
