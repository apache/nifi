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
package com.marklogic.nifi.controller;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.client.ext.SecurityContextType;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Tags({"MarkLogic"})
@CapabilityDescription("Provides a MarkLogic DatabaseClient instance for use by other processors")
public class DefaultDatabaseClientService extends AbstractControllerService implements DatabaseClientService {

    private static List<PropertyDescriptor> properties;

    private DatabaseClient databaseClient;

    protected static Validator NO_VALIDATION_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            return new ValidationResult.Builder().valid(true).build();
        }
    };

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
        .allowableValues(SecurityContextType.BASIC.name(), SecurityContextType.DIGEST.name(),
            SecurityContextType.KERBEROS.name(), SecurityContextType.CERTIFICATE.name())
        .defaultValue(SecurityContextType.DIGEST.name())
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("Username")
        .displayName("Username")
        .description("The user with read, write, or admin privileges - Required for Basic and Digest authentication")
        .addValidator(NO_VALIDATION_VALIDATOR)
        .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
        .name("Password")
        .displayName("Password")
        .description("The password for the user - Required for Basic and Digest authentication")
        .sensitive(true)
        .addValidator(NO_VALIDATION_VALIDATOR)
        .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
        .name("Database")
        .displayName("Database")
        .description("The database to access. By default, the configured database for the REST server would be accessed")
        .addValidator(NO_VALIDATION_VALIDATOR)
        .build();

    public static final PropertyDescriptor CERT_FILE = new PropertyDescriptor.Builder()
        .name("Certificate file")
        .displayName("Certificate file")
        .description("Certificate file which contains the client's certificate chain - Required for " +
            "Certificate authentication")
        .addValidator(NO_VALIDATION_VALIDATOR)
        .build();

    public static final PropertyDescriptor CERT_PASSWORD = new PropertyDescriptor.Builder()
        .name("Certificate password")
        .displayName("Certificate password")
        .description("Export Password of the Certificate file - Optional for Certificate " +
            "authentication")
        .addValidator(NO_VALIDATION_VALIDATOR)
        .build();

    public static final PropertyDescriptor EXTERNAL_NAME = new PropertyDescriptor.Builder()
        .name("External name")
        .displayName("External name")
        .description("External name of the Kerberos Client - Required for Kerberos authentication")
        .addValidator(NO_VALIDATION_VALIDATOR)
        .build();

    static {
        List<PropertyDescriptor> list = new ArrayList<>();
        list.add(HOST);
        list.add(PORT);
        list.add(SECURITY_CONTEXT_TYPE);
        list.add(USERNAME);
        list.add(PASSWORD);
        list.add(DATABASE);
        list.add(CERT_FILE);
        list.add(CERT_PASSWORD);
        list.add(EXTERNAL_NAME);
        properties = Collections.unmodifiableList(list);
    }

    /**
     * TODO Support setting the SSLContext and SSLHostnameVerifier and TrustManager
     */
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
        config.setCertFile(context.getProperty(CERT_FILE).getValue());
        config.setCertPassword(context.getProperty(CERT_PASSWORD).getValue());
        config.setExternalName(context.getProperty(EXTERNAL_NAME).getValue());

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
