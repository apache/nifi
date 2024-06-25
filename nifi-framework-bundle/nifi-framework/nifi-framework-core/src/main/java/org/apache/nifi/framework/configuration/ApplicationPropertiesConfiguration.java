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
package org.apache.nifi.framework.configuration;

import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.util.NiFiBootstrapUtils;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Application Properties Configuration class for Spring Application
 */
@Configuration
public class ApplicationPropertiesConfiguration {
    /**
     * Application Properties
     *
     * @return NiFi Application Properties
     */
    @Bean
    public NiFiProperties nifiProperties() {
        final NiFiPropertiesLoader loader = new NiFiPropertiesLoader();
        final String propertiesPath = NiFiBootstrapUtils.getDefaultApplicationPropertiesFilePath();
        return loader.load(propertiesPath);
    }
}
