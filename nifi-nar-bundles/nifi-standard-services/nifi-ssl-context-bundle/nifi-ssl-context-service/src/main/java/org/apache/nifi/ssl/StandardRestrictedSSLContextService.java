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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * This class is functionally the same as {@link StandardSSLContextService}, but it restricts the allowable
 * values that can be selected for TLS/SSL protocols.
 */
@Tags({"tls", "ssl", "secure", "certificate", "keystore", "truststore", "jks", "p12", "pkcs12", "pkcs"})
@CapabilityDescription("Restricted implementation of the SSLContextService. Provides the ability to configure "
        + "keystore and/or truststore properties once and reuse that configuration throughout the application, "
        + "but only allows a restricted set of TLS/SSL protocols to be chosen (no SSL protocols are supported). The set of protocols selectable will "
        + "evolve over time as new protocols emerge and older protocols are deprecated. This service is recommended "
        + "over StandardSSLContextService if a component doesn't expect to communicate with legacy systems since it is "
        + "unlikely that legacy systems will support these protocols.")
public class StandardRestrictedSSLContextService extends StandardSSLContextService implements RestrictedSSLContextService {

    public static final PropertyDescriptor RESTRICTED_SSL_ALGORITHM = new PropertyDescriptor.Builder()
            .name("SSL Protocol")
            .displayName("TLS Protocol")
            .defaultValue("TLS")
            .required(false)
            .allowableValues(RestrictedSSLContextService.buildAlgorithmAllowableValues())
            .description("The algorithm to use for this SSL context. By default, this will choose the highest supported TLS protocol version.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(KEYSTORE);
        props.add(KEYSTORE_PASSWORD);
        props.add(KEY_PASSWORD);
        props.add(KEYSTORE_TYPE);
        props.add(TRUSTSTORE);
        props.add(TRUSTSTORE_PASSWORD);
        props.add(TRUSTSTORE_TYPE);
        props.add(RESTRICTED_SSL_ALGORITHM);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public String getSslAlgorithm() {
        return configContext.getProperty(RESTRICTED_SSL_ALGORITHM).getValue();
    }

    @Override
    protected String getSSLProtocolForValidation(final ValidationContext validationContext) {
        return validationContext.getProperty(RESTRICTED_SSL_ALGORITHM).getValue();
    }
}
