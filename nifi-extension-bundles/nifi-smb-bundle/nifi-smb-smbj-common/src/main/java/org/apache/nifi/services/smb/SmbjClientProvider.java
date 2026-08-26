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
import com.hierynomus.smbj.auth.GSSAuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.Share;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosUser;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import static org.apache.nifi.smb.common.SmbProperties.AUTHENTICATION_TYPE;
import static org.apache.nifi.smb.common.SmbProperties.AuthenticationType;
import static org.apache.nifi.smb.common.SmbProperties.DOMAIN;
import static org.apache.nifi.smb.common.SmbProperties.HOSTNAME;
import static org.apache.nifi.smb.common.SmbProperties.KERBEROS_USER_SERVICE;
import static org.apache.nifi.smb.common.SmbProperties.PASSWORD;
import static org.apache.nifi.smb.common.SmbProperties.PORT;
import static org.apache.nifi.smb.common.SmbProperties.SHARE;
import static org.apache.nifi.smb.common.SmbProperties.USERNAME;
import static org.apache.nifi.smb.common.SmbUtils.buildSmbClient;

public class SmbjClientProvider implements SmbClientProvider, Closeable {

    private static final String SPNEGO_OID = "1.3.6.1.5.5.2";

    private final PropertyContext context;

    private final ComponentLog logger;

    private final SMBClient smbClient;

    private final KerberosUser kerberosUser;

    public SmbjClientProvider(final PropertyContext context, final ComponentLog logger) {
        this.context = context;
        this.logger = logger;
        this.smbClient = buildSmbClient(context);
        this.kerberosUser = initKerberosUser();
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
            session = connection.authenticate(createAuthenticationContext(logger));
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

    private KerberosUser initKerberosUser() {
        if (getAuthenticationType() == AuthenticationType.KERBEROS) {
            final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);

            final KerberosUser kerberosUser = kerberosUserService.createKerberosUser();
            kerberosUser.login();

            return kerberosUser;
        } else {
            return null;
        }
    }

    private AuthenticationType getAuthenticationType() {
        if (context.getProperty(AUTHENTICATION_TYPE).isSet()) {
            return context.getProperty(AUTHENTICATION_TYPE).asAllowableValue(AuthenticationType.class);
        } else {
            return AuthenticationType.USERNAME_PASSWORD;
        }
    }

    private AuthenticationContext createAuthenticationContext(final ComponentLog logger) {
        return switch (getAuthenticationType()) {
            case USERNAME_PASSWORD -> createUsernamePasswordAuthenticationContext();
            case KERBEROS ->  createKerberosAuthenticationContext(logger);
        };
    }

    private AuthenticationContext createUsernamePasswordAuthenticationContext() {
        if (context.getProperty(USERNAME).isSet()) {
            final String username = context.getProperty(USERNAME).getValue();
            final String password = context.getProperty(PASSWORD).isSet() ? context.getProperty(PASSWORD).getValue() : "";
            final String domain = context.getProperty(DOMAIN).isSet() ? context.getProperty(DOMAIN).getValue() : null;

            return new AuthenticationContext(username, password.toCharArray(), domain);
        } else {
            return AuthenticationContext.anonymous();
        }
    }

    private AuthenticationContext createKerberosAuthenticationContext(final ComponentLog logger) {
        return new KerberosAction<AuthenticationContext>(kerberosUser,
                () -> {
                    final Subject subject = Subject.current();

                    final KerberosPrincipal krbPrincipal = subject.getPrincipals(KerberosPrincipal.class)
                            .iterator()
                            .next();

                    final GSSManager gssManager = GSSManager.getInstance();
                    final GSSName gssName = gssManager.createName(krbPrincipal.getName(), GSSName.NT_USER_NAME);
                    final GSSCredential gssCredential = gssManager.createCredential(gssName, GSSCredential.DEFAULT_LIFETIME, new Oid(SPNEGO_OID), GSSCredential.INITIATE_ONLY);

                    return new GSSAuthenticationContext(krbPrincipal.getName(), krbPrincipal.getRealm(), subject, gssCredential);
                },
                logger)
                .execute();
    }
}
