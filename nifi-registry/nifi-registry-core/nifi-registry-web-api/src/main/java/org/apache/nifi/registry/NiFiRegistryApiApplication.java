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
package org.apache.nifi.registry;

import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.event.StandardEvent;
import org.apache.nifi.registry.hook.Event;
import org.apache.nifi.registry.hook.EventType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * Main class for starting the NiFi Registry Web API as a Spring Boot application.
 *
 * This class is purposely in the org.apache.nifi.registry package since that is the common base
 * package across other modules. This is done because spring-boot will use the package of this
 * class to automatically scan for beans/config/entities/etc. and would otherwise require
 * configuring custom packages to scan in several different places.
 *
 * WebMvcAutoConfiguration is excluded because our web app is using Jersey in place of SpringMVC
 */
@SpringBootApplication
public class NiFiRegistryApiApplication extends SpringBootServletInitializer {

    public static final String NIFI_REGISTRY_PROPERTIES_ATTRIBUTE = "nifi-registry.properties";
    public static final String NIFI_REGISTRY_MASTER_KEY_ATTRIBUTE = "nifi-registry.key";

    @Autowired
    private EventService eventService;

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        final Properties defaultProperties = new Properties();

        // Spring Boot 2.1.0 disabled bean overriding so this re-enables it
        defaultProperties.setProperty("spring.main.allow-bean-definition-overriding", "true");

        // Disable unnecessary Spring MVC filters that cause problems with Jersey
        defaultProperties.setProperty("spring.mvc.hiddenmethod.filter.enabled", "false");
        defaultProperties.setProperty("spring.mvc.formcontent.filter.enabled", "false");

        // Enable Actuator Endpoints
        defaultProperties.setProperty("management.endpoints.web.expose", "*");

        // Run Jersey as a filter instead of a servlet so that requests can be forwarded to other handlers (e.g., actuator)
        defaultProperties.setProperty("spring.jersey.type", "filter");

        return application
                .sources(NiFiRegistryApiApplication.class)
                .properties(defaultProperties);
    }

    @Component
    private class OnApplicationReadyEventing
            implements ApplicationListener<ApplicationReadyEvent> {

        @Override
        public void onApplicationEvent(final ApplicationReadyEvent event) {
            Event registryStartEvent = new StandardEvent.Builder()
                    .eventType(EventType.REGISTRY_START)
                    .build();
            eventService.publish(registryStartEvent);
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(NiFiRegistryApiApplication.class, args);
    }

}
