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
package org.apache.nifi.security.krb;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.RefreshFailedException;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for implementations of KerberosUser.
 * Generally implementations must provide the specific Configuration instance for performing the login,
 * along with an optional CallbackHandler.
 * Some functionality in this class is adapted from Hadoop's UserGroupInformation.
 */
public abstract class AbstractKerberosUser implements KerberosUser {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKerberosUser.class);

    static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DATE_FORMAT);

    /**
     * Percentage of the ticket window to use before we renew the TGT.
     */
    static final float TICKET_RENEW_WINDOW = 0.80f;

    /**
     * The name of the configuration entry to use from the Configuration instance.
     */
    static final String KERBEROS_USER_CONFIG_ENTRY = "KerberosUser";

    protected final String principal;
    protected final AtomicBoolean loggedIn = new AtomicBoolean(false);

    protected Subject subject;
    protected LoginContext loginContext;

    public AbstractKerberosUser(final String principal) {
        this.principal = principal;
        Validate.notBlank(this.principal);
    }

    /**
     * Performs a login using the specified principal and keytab.
     *
     * @throws KerberosLoginException if the login fails
     */
    @Override
    public synchronized void login() {
        if (isLoggedIn()) {
            return;
        }

        try {
            // If it's the first time ever calling login then we need to initialize a new context
            if (loginContext == null) {
                LOGGER.debug("Initializing new login context...");
                if (this.subject == null) {
                    // only create a new subject if a current one does not exist
                    // other classes may be referencing an existing subject and replacing it may break functionality of those other classes after relogin
                    this.subject = new Subject();
                }

                // the Configuration implementations have only one config entry and always return it regardless of the passed in name
                this.loginContext = new LoginContext(KERBEROS_USER_CONFIG_ENTRY, subject, createCallbackHandler(), createConfiguration());
            }

            loginContext.login();
            loggedIn.set(true);
            LOGGER.debug("Successful login for {}", principal);
        } catch (final LoginException le) {
            throw new KerberosLoginException("Unable to login with " + principal + " due to: " + le.getMessage(), le);
        }
    }

    /**
     * Allow sub-classes to provide the Configuration instance.
     *
     * @return the Configuration instance
     */
    protected abstract Configuration createConfiguration();

    /**
     * Allow sub-classes to provide an optional CallbackHandler.
     *
     * @return the CallbackHandler instance, or null
     */
    protected abstract CallbackHandler createCallbackHandler();

    @Override
    public AppConfigurationEntry getConfigurationEntry() {
        final Configuration configuration = createConfiguration();
        final AppConfigurationEntry[] configurationEntries = configuration.getAppConfigurationEntry("KerberosUser");
        if (configurationEntries == null || configurationEntries.length != 1) {
            throw new IllegalStateException("Configuration must return one entry");
        }
        return configurationEntries[0];
    }

    /**
     * Performs a logout of the current user.
     *
     * @throws KerberosLoginException if the logout fails
     */
    @Override
    public synchronized void logout() {
        if (!isLoggedIn()) {
            return;
        }

        try {
            loginContext.logout();
            loggedIn.set(false);
            LOGGER.debug("Successful logout for {}", principal);

            loginContext = null;
        } catch (final LoginException e) {
            throw new KerberosLoginException("Logout failed due to: " + e.getMessage(), e);
        }
    }

    /**
     * Executes the PrivilegedAction as this user.
     *
     * @param action the action to execute
     * @param <T> the type of result
     * @return the result of the action
     * @throws IllegalStateException if this method is called while not logged in
     */
    @Override
    public <T> T doAs(final PrivilegedAction<T> action) throws IllegalStateException {
        if (!isLoggedIn()) {
            throw new IllegalStateException("Must login before executing actions");
        }

        return Subject.callAs(subject, action::run);
    }

    /**
     * Executes the PrivilegedAction as this user.
     *
     * @param action the action to execute
     * @param <T> the type of result
     * @return the result of the action
     * @throws IllegalStateException if this method is called while not logged in
     * @throws PrivilegedActionException if an exception is thrown from the action
     */
    @Override
    public <T> T doAs(final PrivilegedExceptionAction<T> action)
            throws IllegalStateException, PrivilegedActionException {
        if (!isLoggedIn()) {
            throw new IllegalStateException("Must login before executing actions");
        }

        return Subject.callAs(subject, action::run);
    }

    /**
     * Re-login a user from keytab if TGT is expired or is close to expiry.
     *
     */
    @Override
    public synchronized boolean checkTGTAndRelogin()  {
        final KerberosTicket tgt = getTGT();
        if (tgt == null) {
            LOGGER.debug("TGT for {} was not found", principal);
            return logoutAndLogin();
        }

        if (System.currentTimeMillis() < getRefreshTime(tgt)) {
            LOGGER.debug("TGT for {} was found, but has not reached expiration window", principal);
            return false;
        }

        if (!tgt.isRenewable() || tgt.getRenewTill() == null) {
            return logoutAndLogin();
        }

        LOGGER.debug("TGT for {} is renewable, will attempt refresh", principal);
        try {
            tgt.refresh();
            LOGGER.debug("TGT for {} was refreshed", principal);
            return true;
        } catch (final RefreshFailedException e) {
            LOGGER.debug("TGT for {} could not be refreshed", principal);
            LOGGER.trace("", e);
            return logoutAndLogin();
        }
    }

    private boolean logoutAndLogin() {
        LOGGER.debug("Performing logout/login {}", principal);
        logout();
        login();
        return true;
    }

    /**
     * Get the Kerberos TGT.
     *
     * @return the user's TGT or null if none was found
     */
    private synchronized KerberosTicket getTGT() {
        final Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);

        for (KerberosTicket ticket : tickets) {
            if (isTGSPrincipal(ticket.getServer())) {
                return ticket;
            }
        }

        return null;
    }

    /**
     * TGS must have the server principal of the form "krbtgt/FOO@FOO".
     *
     * @param principal the principal to check
     * @return true if the principal is the TGS, false otherwise
     */
    private boolean isTGSPrincipal(final KerberosPrincipal principal) {
        if (principal == null) {
            return false;
        }

        if (principal.getName().equals("krbtgt/" + principal.getRealm() + "@" + principal.getRealm())) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Found TGS principal: {}", principal.getName());
            }
            return true;
        }

        return false;
    }

    private long getRefreshTime(final KerberosTicket tgt) {
        final long start = tgt.getStartTime().getTime();
        final long end = tgt.getEndTime().getTime();

        if (LOGGER.isTraceEnabled()) {
            final String startDate = DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(start));
            final String endDate = DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(end));
            LOGGER.trace("TGT for {} is valid starting at [{}]", principal, startDate);
            LOGGER.trace("TGT for {} expires at [{}]", principal, endDate);
            if (tgt.getRenewTill() == null) {
                LOGGER.trace("TGT for {} is non-renewable", principal);
            } else {
                LOGGER.trace("TGT for {} renews until [{}]", principal,  DATE_TIME_FORMATTER.format(tgt.getRenewTill().toInstant()));
            }
        }

        return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
    }

    /**
     * @return true if this user is currently logged in, false otherwise
     */
    @Override
    public boolean isLoggedIn() {
        return loggedIn.get();
    }

    /**
     * @return the principal for this user
     */
    @Override
    public String getPrincipal() {
        return principal;
    }

    // Visible for testing
    Subject getSubject() {
        return this.subject;
    }

    @Override
    public String toString() {
        return "KerberosUser{" +
                "principal='" + principal + '\'' +
                ", loggedIn=" + loggedIn +
                '}';
    }
}
