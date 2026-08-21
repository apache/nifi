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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.smb.common.SmbProperties.DOMAIN;
import static org.apache.nifi.smb.common.SmbProperties.ENABLE_DFS;
import static org.apache.nifi.smb.common.SmbProperties.HOSTNAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_DOMAIN_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_ENABLE_DFS_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_HOSTNAME_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_PASSWORD_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_PORT_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_SHARE_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_SMB_DIALECT_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_TIMEOUT_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_USERNAME_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_USE_ENCRYPTION_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.PASSWORD;
import static org.apache.nifi.smb.common.SmbProperties.PORT;
import static org.apache.nifi.smb.common.SmbProperties.SHARE;
import static org.apache.nifi.smb.common.SmbProperties.SMB_DIALECT;
import static org.apache.nifi.smb.common.SmbProperties.TIMEOUT;
import static org.apache.nifi.smb.common.SmbProperties.USERNAME;
import static org.apache.nifi.smb.common.SmbProperties.USE_ENCRYPTION;

@Tags({"samba, smb, cifs, files"})
@CapabilityDescription("Provides access to SMB Sessions with shared authentication credentials.")
public class SmbjClientProviderService extends AbstractControllerService implements SmbClientProviderService {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            HOSTNAME,
            PORT,
            SHARE,
            USERNAME,
            PASSWORD,
            DOMAIN,
            SMB_DIALECT,
            USE_ENCRYPTION,
            ENABLE_DFS,
            TIMEOUT
    );

    private SmbjClientProvider delegate;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        delegate = new SmbjClientProvider(context, getLogger());
    }

    @OnDisabled
    public void onDisabled() {
        if (delegate != null) {
            try {
                delegate.close();
            } catch (Exception e) {
                getLogger().error("Error while closing SMB ClientProvider", e);
            } finally {
                delegate = null;
            }
        }
    }

    @Override
    public URI getServiceLocation(final Map<String, String> attributes) {
        return delegate.getServiceLocation(attributes);
    }

    @Override
    public SmbClientService getClient(final ComponentLog logger, final Map<String, String> attributes) throws IOException {
        return delegate.getClient(logger, attributes);
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.renameProperty(OLD_HOSTNAME_PROPERTY_NAME, HOSTNAME.getName());
        config.renameProperty(OLD_DOMAIN_PROPERTY_NAME, DOMAIN.getName());
        config.renameProperty(OLD_USERNAME_PROPERTY_NAME, USERNAME.getName());
        config.renameProperty(OLD_PASSWORD_PROPERTY_NAME, PASSWORD.getName());
        config.renameProperty(OLD_PORT_PROPERTY_NAME, PORT.getName());
        config.renameProperty(OLD_SHARE_PROPERTY_NAME, SHARE.getName());
        config.renameProperty(OLD_ENABLE_DFS_PROPERTY_NAME, ENABLE_DFS.getName());
        config.renameProperty(OLD_SMB_DIALECT_PROPERTY_NAME, SMB_DIALECT.getName());
        config.renameProperty(OLD_TIMEOUT_PROPERTY_NAME, TIMEOUT.getName());
        config.renameProperty(OLD_USE_ENCRYPTION_PROPERTY_NAME, USE_ENCRYPTION.getName());
    }
}
