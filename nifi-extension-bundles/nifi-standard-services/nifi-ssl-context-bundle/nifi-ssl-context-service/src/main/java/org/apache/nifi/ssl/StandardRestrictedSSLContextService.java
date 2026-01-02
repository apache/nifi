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
package org.apache.nifi.ssl;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.TlsPlatform;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is functionally the same as {@link StandardSSLContextService}, but it restricts the allowable
 * values that can be selected for TLS/SSL protocols.
 */
@DeprecationNotice(
        reason = "No longer provides differentiated security features",
        alternatives = {
                PEMEncodedSSLContextProvider.class,
                StandardSSLContextService.class
        }
)
@Tags({"tls", "ssl", "secure", "certificate", "keystore", "truststore", "jks", "p12", "pkcs12", "pkcs"})
@CapabilityDescription("Restricted implementation of the SSLContextService. Provides the ability to configure "
        + "keystore and/or truststore properties once and reuse that configuration throughout the application, "
        + "but only allows a restricted set of TLS/SSL protocols to be chosen (no SSL protocols are supported). The set of protocols selectable will "
        + "evolve over time as new protocols emerge and older protocols are deprecated. This service is recommended "
        + "over StandardSSLContextService if a component doesn't expect to communicate with legacy systems since it is "
        + "unlikely that legacy systems will support these protocols.")
public class StandardRestrictedSSLContextService extends StandardSSLContextService implements RestrictedSSLContextService {

    public static final PropertyDescriptor RESTRICTED_SSL_ALGORITHM = new PropertyDescriptor.Builder()
            .name("TLS Protocol")
            .defaultValue(TLS_PROTOCOL)
            .required(false)
            .allowableValues(getRestrictedProtocolAllowableValues())
            .description("TLS Protocol Version for encrypted connections. Supported versions depend on the specific version of Java used.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        KEYSTORE,
        KEYSTORE_PASSWORD,
        KEY_PASSWORD,
        KEYSTORE_TYPE,
        TRUSTSTORE,
        TRUSTSTORE_PASSWORD,
        TRUSTSTORE_TYPE,
        RESTRICTED_SSL_ALGORITHM);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public String getSslAlgorithm() {
        return configContext.getProperty(RESTRICTED_SSL_ALGORITHM).getValue();
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        config.renameProperty("SSL Protocol", RESTRICTED_SSL_ALGORITHM.getName());
    }

    private static AllowableValue[] getRestrictedProtocolAllowableValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>();

        allowableValues.add(new AllowableValue(TLS_PROTOCOL, TLS_PROTOCOL, "Negotiate latest protocol version based on platform supported versions"));

        for (final String preferredProtocol : TlsPlatform.getPreferredProtocols()) {
            final String description = String.format("Require %s protocol version", preferredProtocol);
            allowableValues.add(new AllowableValue(preferredProtocol, preferredProtocol, description));
        }

        return allowableValues.toArray(new AllowableValue[allowableValues.size()]);
    }
}
