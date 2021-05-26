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

import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.context.ServletContextAware;

import javax.servlet.ServletContext;

/**
 * The JettyServer puts an instance of NiFiRegistryProperties into the ServletContext, this class
 * obtains that instance and makes it available to inject to all other places.
 *
 */
@Configuration
public class NiFiRegistryPropertiesFactory implements ServletContextAware {

    private NiFiRegistryProperties properties;

    @Override
    public void setServletContext(ServletContext servletContext) {
        properties = (NiFiRegistryProperties) servletContext.getAttribute(
                NiFiRegistryApiApplication.NIFI_REGISTRY_PROPERTIES_ATTRIBUTE);
    }

    @Bean
    public NiFiRegistryProperties getNiFiRegistryProperties() {
        return properties;
    }

}
