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
package org.apache.nifi.marklogic.controller;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.client.ext.SecurityContextType;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Tags({"MarkLogic"})
@CapabilityDescription("Provides a MarkLogic DatabaseClient instance for use by other processors")
public class DefaultMarkLogicDatabaseClientService extends AbstractControllerService implements MarkLogicDatabaseClientService {

    private static List<PropertyDescriptor> properties;

    private DatabaseClient databaseClient;

    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
        .name("Host")
        .displayName("Host")
        .required(true)
        .defaultValue("localhost")
        .description("The host with the REST server for which a DatabaseClient instance needs to be created")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
        .name("Port")
        .displayName("Port")
        .required(true)
        .defaultValue("8000")
        .description("The port on which the REST server is hosted")
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .build();

    public static final PropertyDescriptor SECURITY_CONTEXT_TYPE = new PropertyDescriptor.Builder()
        .name("Security Context Type")
        .displayName("Security Context Type")
        .required(true)
        .allowableValues(SecurityContextType.values())
        .description("The type of the Security Context that needs to be used for authentication")
        .allowableValues(SecurityContextType.BASIC.name(), SecurityContextType.DIGEST.name(), SecurityContextType.CERTIFICATE.name())
 //     TODO: Add support for Kerberos authentication after testing
 //     , SecurityContextType.KERBEROS.name())
        .defaultValue(SecurityContextType.DIGEST.name())
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("Username")
        .displayName("Username")
        .description("The user with read, write, or admin privileges - Required for Basic and Digest authentication")
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
        .name("Password")
        .displayName("Password")
        .description("The password for the user - Required for Basic and Digest authentication")
        .sensitive(true)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
        .name("Database")
        .displayName("Database")
        .description("The database to access. By default, the configured database for the REST server would be accessed")
        .addValidator(Validator.VALID)
        .build();

/*
    public static final PropertyDescriptor EXTERNAL_NAME = new PropertyDescriptor.Builder()
        .name("External name")
        .displayName("External name")
        .description("External name of the Kerberos Client - Required for Kerberos authentication")
        .addValidator(Validator.VALID)
        .build();
*/
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide KeyStore and TrustManager information for secure connections")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("Client Authentication")
            .displayName("Client Authentication")
            .description("Client authentication policy when connecting via a secure connection. This property is only used when an SSL Context "
                    + "has been defined and enabled")
            .required(false)
            .allowableValues(SSLContextService.ClientAuth.values())
            .build();

    static {
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(HOST);
        list.add(PORT);
        list.add(SECURITY_CONTEXT_TYPE);
        list.add(USERNAME);
        list.add(PASSWORD);
        list.add(DATABASE);
        // list.add(EXTERNAL_NAME);
        list.add(SSL_CONTEXT_SERVICE);
        list.add(CLIENT_AUTH);
        properties = Collections.unmodifiableList(list);
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        DatabaseClientConfig config = new DatabaseClientConfig();
        config.setHost(context.getProperty(HOST).getValue());
        config.setPort(context.getProperty(PORT).asInteger());
        config.setSecurityContextType(SecurityContextType.valueOf(
            context.getProperty(SECURITY_CONTEXT_TYPE).getValue())
        );
        config.setUsername(context.getProperty(USERNAME).getValue());
        config.setPassword(context.getProperty(PASSWORD).getValue());
        config.setDatabase(context.getProperty(DATABASE).getValue());
        // config.setExternalName(context.getProperty(EXTERNAL_NAME).getValue());
        final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if(sslService != null) {
            final SSLContextService.ClientAuth clientAuth;
            try {
                clientAuth =
                        context.getProperty(CLIENT_AUTH).getValue() == null ? SSLContextService.ClientAuth.REQUIRED :
                                SSLContextService.ClientAuth.valueOf(context.getProperty(CLIENT_AUTH).getValue());
            } catch (IllegalArgumentException exception) {
                throw new ProviderCreationException("Client Authentication should be one of the following values : "
                        + Arrays.toString(SSLContextService.ClientAuth.values()));
            }
            try {
                if (sslService.isTrustStoreConfigured()) {
                    final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    final KeyStore trustStore = KeyStoreUtils.getTrustStore(sslService.getTrustStoreType());
                    try (final InputStream is = new FileInputStream(sslService.getTrustStoreFile())) {
                        trustStore.load(is, sslService.getTrustStorePassword().toCharArray());
                    }
                    trustManagerFactory.init(trustStore);
                    config.setTrustManager((X509TrustManager) trustManagerFactory.getTrustManagers()[0]);
                }
                final SSLContext sslContext = sslService.createSSLContext(clientAuth);
                config.setSslContext(sslContext);
            } catch (Exception e) {
                getLogger().error("Failed to create SSLContext due to {}", new Object[]{e} );
                throw new ProcessException(e);
            }
        }
        getLogger().info("Creating DatabaseClient");
        databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(config);
    }

    @OnDisabled
    public void shutdown() {
        if (databaseClient != null) {
            databaseClient.release();
        }
    }

    @Override
    public DatabaseClient getDatabaseClient() {
        return databaseClient;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
