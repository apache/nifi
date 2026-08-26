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
package org.apache.nifi.processors.smb.util;

import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.smb.common.SmbProperties;

public class LocalSmbProperties {

    private static final String DEPRECATION_TAG = " Deprecation notice: Local processor properties used to configure the SMB connection are deprecated" +
            " and will be removed in the next major release. Use SMB Client Provider Service instead.";

    public enum ConnectionConfigurationStrategy implements DescribedValue {
        CONTROLLER_SERVICE("Controller Service", "Use SMB Client Provider Service to configure the SMB connection."),
        LOCAL_PROPERTIES("Local Properties", "Use local processor properties to configure the SMB connection." + DEPRECATION_TAG);

        private final String displayName;
        private final String description;

        ConnectionConfigurationStrategy(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return displayName;
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    public static final PropertyDescriptor CONNECTION_CONFIGURATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Connection Configuration Strategy")
            .description("Specifies whether SMB Client Provider Service or local processor properties are used to configure the SMB connection.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(ConnectionConfigurationStrategy.class)
            .defaultValue(ConnectionConfigurationStrategy.LOCAL_PROPERTIES)
            .build();

    public static final PropertyDescriptor SMB_CLIENT_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("SMB Client Provider Service")
            .description("Specifies the SMB client provider to use for creating SMB connections.")
            .required(true)
            .identifiesControllerService(SmbClientProviderService.class)
            .dependsOn(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.CONTROLLER_SERVICE)
            .build();

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SmbProperties.HOSTNAME)
            .description(SmbProperties.HOSTNAME.getDescription() + DEPRECATION_TAG)
            .dependsOn(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.LOCAL_PROPERTIES)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SmbProperties.PORT)
            .description(SmbProperties.PORT.getDescription() + DEPRECATION_TAG)
            .dependsOn(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.LOCAL_PROPERTIES)
            .build();

    public static final PropertyDescriptor SHARE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SmbProperties.SHARE)
            .description(SmbProperties.SHARE.getDescription() + DEPRECATION_TAG)
            .dependsOn(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.LOCAL_PROPERTIES)
            .build();

    public static final PropertyDescriptor DOMAIN = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SmbProperties.DOMAIN)
            .description(SmbProperties.DOMAIN.getDescription() + DEPRECATION_TAG)
            .clearDependsOn()
            .dependsOn(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.LOCAL_PROPERTIES)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SmbProperties.USERNAME)
            .description(SmbProperties.USERNAME.getDescription() + DEPRECATION_TAG)
            .clearDependsOn()
            .dependsOn(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.LOCAL_PROPERTIES)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SmbProperties.PASSWORD)
            .description(SmbProperties.PASSWORD.getDescription() + DEPRECATION_TAG)
            .clearDependsOn()
            .dependsOn(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.LOCAL_PROPERTIES)
            .build();

    public static final PropertyDescriptor SMB_DIALECT = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SmbProperties.SMB_DIALECT)
            .description(SmbProperties.SMB_DIALECT.getDescription() + DEPRECATION_TAG)
            .dependsOn(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.LOCAL_PROPERTIES)
            .build();

    public static final PropertyDescriptor USE_ENCRYPTION = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SmbProperties.USE_ENCRYPTION)
            .description(SmbProperties.USE_ENCRYPTION.getDescription() + DEPRECATION_TAG)
            .dependsOn(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.LOCAL_PROPERTIES)
            .build();

    public static final PropertyDescriptor ENABLE_DFS = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SmbProperties.ENABLE_DFS)
            .description(SmbProperties.ENABLE_DFS.getDescription() + DEPRECATION_TAG)
            .dependsOn(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.LOCAL_PROPERTIES)
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SmbProperties.TIMEOUT)
            .description(SmbProperties.TIMEOUT.getDescription() + DEPRECATION_TAG)
            .dependsOn(CONNECTION_CONFIGURATION_STRATEGY, ConnectionConfigurationStrategy.LOCAL_PROPERTIES)
            .build();

}
