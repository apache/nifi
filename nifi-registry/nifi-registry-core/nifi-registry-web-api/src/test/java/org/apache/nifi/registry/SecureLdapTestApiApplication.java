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

import org.apache.nifi.registry.db.DataSourceFactory;
import org.apache.nifi.registry.security.authorization.AuthorizerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication
@ComponentScan(
        basePackages = "org.apache.nifi.registry",
        excludeFilters = {
                @ComponentScan.Filter(
                        type = FilterType.ASSIGNABLE_TYPE,
                        value = SpringBootServletInitializer.class), // Avoid loading NiFiRegistryApiApplication
                @ComponentScan.Filter(
                        type = FilterType.ASSIGNABLE_TYPE,
                        value = DataSourceFactory.class), // Avoid loading DataSourceFactory
                @ComponentScan.Filter(
                        type = FilterType.ASSIGNABLE_TYPE,
                        value = AuthorizerFactory.class), // Avoid loading AuthorizerFactory.getAuthorizer(), as we need to add it again with test-specific @DependsOn annotation
                @ComponentScan.Filter(
                        type = FilterType.REGEX,
                        pattern = "org\\.apache\\.nifi\\.registry\\.NiFiRegistryPropertiesFactory"), // Avoid loading NiFiRegistryPropertiesFactory
        })
public class SecureLdapTestApiApplication extends SpringBootServletInitializer {

}
