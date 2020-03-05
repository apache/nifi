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
package org.apache.nifi.schemaregistry.hortonworks;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.util.Map;

/**
 * Extend the SchemaRegistryClient so we can override the initialization of the security context and use
 * the KerberosUserLogin implementation that lets us login with a principal/password.
 */
public class SchemaRegistryClientWithKerberosPassword extends SchemaRegistryClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryClientWithKerberosPassword.class);

    public static final String SCHEMA_REGISTRY_CLIENT_KERBEROS_PRINCIPAL = "schema.registry.client.kerberos.principal";
    public static final String SCHEMA_REGISTRY_CLIENT_KERBEROS_PASSWORD = "schema.registry.client.kerberos.password";
    public static final String SCHEMA_REGISTRY_CLIENT_NIFI_COMP_LOGGER = "schema.registry.client.nifi.component.logger";

    private KerberosUser kerberosUser;

    public SchemaRegistryClientWithKerberosPassword(final Map<String, ?> conf) {
        super(conf);
    }

    @Override
    protected void initializeSecurityContext() {
        final String principal = configuration.getValue(SCHEMA_REGISTRY_CLIENT_KERBEROS_PRINCIPAL);
        if (principal == null) {
            throw new IllegalArgumentException("Failed to login because principal is null");
        }

        final String password = configuration.getValue(SCHEMA_REGISTRY_CLIENT_KERBEROS_PASSWORD);
        if (password == null) {
            throw new IllegalArgumentException("Failed to login because password is null");
        }

        final Object loggerObject = configuration.getValue(SCHEMA_REGISTRY_CLIENT_NIFI_COMP_LOGGER);
        if (loggerObject == null) {
            throw new IllegalArgumentException("Failed to login because component logger is required");
        }

        if (!(loggerObject instanceof ComponentLog)) {
            throw new IllegalArgumentException("Failed to login because logger object is not a ComponentLog");
        }

        kerberosUser = new KerberosPasswordUser(principal, password);
        login = new KerberosUserLogin(kerberosUser, (ComponentLog) loggerObject);

        try {
            login.login();
        } catch (LoginException e) {
            LOGGER.error("Failed to login as principal `{}`", new Object[]{principal}, e);
        }
    }

    @Override
    public void close() {
        try {
            kerberosUser.logout();
        } catch (Throwable t) {
            LOGGER.error("Error performing logout of principal during close(): " + t.getMessage(), t);
        } finally {
            kerberosUser = null;
        }

        super.close();
    }
}
