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
package org.apache.nifi.registry.db;

import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.mockito.Mockito;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * Sets up the application context for database repository tests.
 *
 * The @SpringBootTest annotation on the repository tests will find this class by working up the package hierarchy.
 * This class must be in the "db" package in order to find the entities in "db.entity" and repositories in "db.repository".
 *
 * The DataSourceFactory is excluded so that Spring Boot will load an in-memory H2 database.
 */
@SpringBootApplication
@ComponentScan(
        excludeFilters = {
                @ComponentScan.Filter(
                        type = FilterType.ASSIGNABLE_TYPE,
                        value = DataSourceFactory.class)
        })
public class DatabaseTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(DatabaseTestApplication.class, args);
    }

    @Bean
    public NiFiRegistryProperties createNiFiRegistryProperties() {
        return Mockito.mock(NiFiRegistryProperties.class);
    }

}
