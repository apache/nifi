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

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.Share;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.logging.ComponentLog;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static org.apache.nifi.smb.common.SmbProperties.DOMAIN;
import static org.apache.nifi.smb.common.SmbProperties.HOSTNAME;
import static org.apache.nifi.smb.common.SmbProperties.PASSWORD;
import static org.apache.nifi.smb.common.SmbProperties.PORT;
import static org.apache.nifi.smb.common.SmbProperties.SHARE;
import static org.apache.nifi.smb.common.SmbProperties.USERNAME;
import static org.apache.nifi.smb.common.SmbUtils.buildSmbClient;

public class SmbjClientProvider implements SmbClientProvider, Closeable {

    private final PropertyContext context;

    private final ComponentLog logger;

    private final SMBClient smbClient;

    private AuthenticationContext authenticationContext;

    public SmbjClientProvider(final PropertyContext context, final ComponentLog logger) {
        this.context = context;
        this.logger = logger;
        this.smbClient = buildSmbClient(context);
        createAuthenticationContext(context);
    }

    @Override
    public void close() {
        smbClient.close();
    }

    private String getHostname(final Map<String, String> attributes) {
        return context.getProperty(HOSTNAME).evaluateAttributeExpressions(attributes).getValue();
    }

    private Integer getPort(final Map<String, String> attributes) {
        return context.getProperty(PORT).evaluateAttributeExpressions(attributes).asInteger();
    }

    private String getShareName(final Map<String, String> attributes) {
        return context.getProperty(SHARE).evaluateAttributeExpressions(attributes).getValue();
    }

    @Override
    public URI getServiceLocation(final Map<String, String> attributes) {
        return URI.create(String.format("smb://%s:%d/%s", getHostname(attributes), getPort(attributes), getShareName(attributes)));
    }

    @Override
    public SmbClientService getClient(final ComponentLog logger, final Map<String, String> attributes) throws IOException {
        final Connection connection = smbClient.connect(getHostname(attributes), getPort(attributes));
        final URI serviceLocation = getServiceLocation(attributes);

        final Session session;
        final Share share;

        try {
            session = connection.authenticate(authenticationContext);
        } catch (Exception e) {
            throw new IOException("Could not create session for share " + serviceLocation, e);
        }

        try {
            share = session.connectShare(getShareName(attributes));
        } catch (Exception e) {
            closeSession(session, serviceLocation);
            throw new IOException("Could not connect to share " + serviceLocation, e);
        }

        if (!(share instanceof DiskShare)) {
            closeSession(session, serviceLocation);
            throw new IllegalArgumentException("DiskShare not found. Share " + share.getClass().getSimpleName() + " found on " + serviceLocation);
        }

        return new SmbjClientService(session, (DiskShare) share, getServiceLocation(attributes), logger);
    }


    private void closeSession(final Session session, URI serviceLocation) {
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            logger.error("Could not close session to {}", serviceLocation, e);
        }
    }

    private void createAuthenticationContext(final PropertyContext context) {
        if (context.getProperty(USERNAME).isSet()) {
            final String userName = context.getProperty(USERNAME).getValue();
            final String password =
                    context.getProperty(PASSWORD).isSet() ? context.getProperty(PASSWORD).getValue() : "";
            final String domainOrNull =
                    context.getProperty(DOMAIN).isSet() ? context.getProperty(DOMAIN).getValue() : null;
            authenticationContext = new AuthenticationContext(userName, password.toCharArray(), domainOrNull);
        } else {
            authenticationContext = AuthenticationContext.anonymous();
        }
    }
}
