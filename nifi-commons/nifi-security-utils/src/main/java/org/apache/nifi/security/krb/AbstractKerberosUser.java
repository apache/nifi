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

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractKerberosUser implements KerberosUser {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKerberosUser.class);

    static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    /**
     * Percentage of the ticket window to use before we renew the TGT.
     */
    static final float TICKET_RENEW_WINDOW = 0.80f;

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
     * @throws LoginException if the login fails
     */
    @Override
    public synchronized void login() throws LoginException {
        if (isLoggedIn()) {
            return;
        }

        try {
            // If it's the first time ever calling login then we need to initialize a new context
            if (loginContext == null) {
                LOGGER.debug("Initializing new login context...");
                this.subject = new Subject();
                this.loginContext = createLoginContext(subject);
            }

            loginContext.login();
            loggedIn.set(true);
            LOGGER.debug("Successful login for {}", new Object[]{principal});
        } catch (LoginException le) {
            throw new LoginException("Unable to login with " + principal + " due to: " + le.getMessage());
        }
    }

    protected abstract LoginContext createLoginContext(final Subject subject) throws LoginException;

    /**
     * Performs a logout of the current user.
     *
     * @throws LoginException if the logout fails
     */
    @Override
    public synchronized void logout() throws LoginException {
        if (!isLoggedIn()) {
            return;
        }

        try {
            loginContext.logout();
            loggedIn.set(false);
            LOGGER.debug("Successful logout for {}", new Object[]{principal});

            subject = null;
            loginContext = null;
        } catch (LoginException e) {
            throw new LoginException("Logout failed due to: " + e.getMessage());
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

        return Subject.doAs(subject, action);
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

        return Subject.doAs(subject, action);
    }

    /**
     * Re-login a user from keytab if TGT is expired or is close to expiry.
     *
     * @throws LoginException if an error happens performing the re-login
     */
    @Override
    public synchronized boolean checkTGTAndRelogin() throws LoginException {
        final KerberosTicket tgt = getTGT();
        if (tgt == null) {
            LOGGER.debug("TGT was not found");
        }

        if (tgt != null && System.currentTimeMillis() < getRefreshTime(tgt)) {
            LOGGER.debug("TGT was found, but has not reached expiration window");
            return false;
        }

        LOGGER.debug("Performing relogin for {}", new Object[]{principal});
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
                LOGGER.trace("Found TGT principal: " + principal.getName());
            }
            return true;
        }

        return false;
    }

    private long getRefreshTime(final KerberosTicket tgt) {
        long start = tgt.getStartTime().getTime();
        long end = tgt.getEndTime().getTime();

        if (LOGGER.isTraceEnabled()) {
            final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
            final String startDate = dateFormat.format(new Date(start));
            final String endDate = dateFormat.format(new Date(end));
            LOGGER.trace("TGT valid starting at: " + startDate);
            LOGGER.trace("TGT expires at: " + endDate);
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

}
