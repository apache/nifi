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
package org.apache.nifi.services.smb;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.processor.util.StandardValidators.INTEGER_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.PORT_VALIDATOR;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.transport.tcp.async.AsyncDirectTcpTransportFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

@Tags({"microsoft", "samba"})
@CapabilityDescription("Provides connection pool for ListSmb processor. ")
public class SmbjConnectionPoolService extends AbstractControllerService implements SmbConnectionPoolService {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .displayName("Hostname")
            .name("hostname")
            .description("The network host of the SMB file server.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor DOMAIN = new PropertyDescriptor.Builder()
            .displayName("Domain")
            .name("domain")
            .description(
                    "The domain used for authentication. Optional, in most cases username and password is sufficient.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .displayName("Username")
            .name("username")
            .description(
                    "The username used for authentication.")
            .required(false)
            .defaultValue("Guest")
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .displayName("Password")
            .name("password")
            .description("The password used for authentication.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .displayName("Port")
            .name("port")
            .description("Port to use for connection.")
            .required(true)
            .addValidator(PORT_VALIDATOR)
            .defaultValue("445")
            .build();
    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .displayName("Timeout")
            .name("timeout")
            .description("Timeout in seconds for read and write operations.")
            .required(true)
            .defaultValue("5")
            .addValidator(INTEGER_VALIDATOR)
            .build();
    private static final List<PropertyDescriptor> PROPERTIES = Collections
            .unmodifiableList(asList(
                    HOSTNAME,
                    DOMAIN,
                    USERNAME,
                    PASSWORD,
                    PORT,
                    TIMEOUT
            ));
    private SMBClient smbClient;
    private AuthenticationContext authenticationContext;
    private ConfigurationContext context;
    private String hostname;
    private Integer port;

    @Override
    public Session getSession() {
        try {
            return smbClient.connect(hostname, port).authenticate(authenticationContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public URI getServiceLocation() {
        return URI.create(String.format("smb://%s:%d", hostname, port));
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) throws IOException {
        this.context = context;
        this.hostname = context.getProperty(HOSTNAME).getValue();
        this.port = context.getProperty(PORT).asInteger();
        this.smbClient = new SMBClient(SmbConfig.builder()
                .withTimeout(context.getProperty(TIMEOUT).asLong(), SECONDS)
                .withTransportLayerFactory(new AsyncDirectTcpTransportFactory<>())
                .build());
        createAuthenticationContext();
    }

    @OnDisabled
    public void onDisabled() {
        smbClient.close();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final String hostname = validationContext.getProperty(HOSTNAME).getValue();
        final String port = validationContext.getProperty(PORT).getValue();
        final String timeout = validationContext.getProperty(TIMEOUT).getValue();

        return asList(HOSTNAME.validate(hostname, validationContext),
                PORT.validate(port, validationContext),
                TIMEOUT.validate(timeout, validationContext)
        );
    }

    private void createAuthenticationContext() {
        if (context.getProperty(USERNAME).isSet()) {
            final String userName = context.getProperty(USERNAME).getValue();
            final String password = context.getProperty(PASSWORD).isSet() ? context.getProperty(PASSWORD).getValue() : "";
            final String domainOrNull = context.getProperty(DOMAIN).isSet() ? context.getProperty(DOMAIN).getValue() : null;
            authenticationContext = new AuthenticationContext(userName, password.toCharArray(), domainOrNull);
        } else {
            authenticationContext = AuthenticationContext.anonymous();
        }
    }

}
