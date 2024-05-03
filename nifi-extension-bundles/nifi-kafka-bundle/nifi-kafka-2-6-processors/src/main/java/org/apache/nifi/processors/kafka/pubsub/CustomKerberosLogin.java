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
package org.apache.nifi.processors.kafka.pubsub;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.authenticator.AbstractLogin;
import org.apache.kafka.common.security.kerberos.KerberosLogin;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.RefreshFailedException;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Customized version of {@link org.apache.kafka.common.security.kerberos.KerberosLogin} which improves the re-login logic
 * to avoid making system calls to kinit when the ticket cache is being used, and to avoid exiting the refresh thread so that
 * it may recover if the ticket cache is externally refreshed.
 * The re-login thread follows a similar approach used by NiFi's KerberosUser which attempts to call tgt.refresh()
 * and falls back to a logout/login.
 * The Kafka client is configured to use this login by setting SaslConfigs.SASL_LOGIN_CLASS
 * when the SASL mechanism is GSSAPI.
 */
public class CustomKerberosLogin extends AbstractLogin {
    private static final Logger log = LoggerFactory.getLogger(CustomKerberosLogin.class);

    private Thread refreshThread;
    private boolean isKrbTicket;

    private String principal;

    private double ticketRenewWindowFactor;
    private long minTimeBeforeRelogin;

    private volatile Subject subject;

    private LoginContext loginContext;
    private String serviceName;

    @Override
    public void configure(Map<String, ?> configs, String contextName, Configuration configuration,
                          AuthenticateCallbackHandler callbackHandler) {
        super.configure(configs, contextName, configuration, callbackHandler);
        this.ticketRenewWindowFactor = (Double) configs.get(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR);
        this.minTimeBeforeRelogin = (Long) configs.get(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN);
        this.serviceName = getServiceName(configs, contextName, configuration);
    }

    /**
     * Performs login for each login module specified for the login context of this instance and starts the thread used
     * to periodically re-login to the Kerberos Ticket Granting Server.
     */
    @Override
    public LoginContext login() throws LoginException {
        loginContext = super.login();
        subject = loginContext.getSubject();
        isKrbTicket = !subject.getPrivateCredentials(KerberosTicket.class).isEmpty();

        AppConfigurationEntry[] entries = configuration().getAppConfigurationEntry(contextName());
        if (entries.length == 0) {
            principal = null;
        } else {
            // there will only be a single entry
            AppConfigurationEntry entry = entries[0];
            if (entry.getOptions().get("principal") != null)
                principal = (String) entry.getOptions().get("principal");
            else
                principal = null;
        }

        if (!isKrbTicket) {
            log.debug("[Principal={}]: It is not a Kerberos ticket", principal);
            refreshThread = null;
            // if no TGT, do not bother with ticket management.
            return loginContext;
        }
        log.debug("[Principal={}]: It is a Kerberos ticket", principal);

        refreshThread = KafkaThread.daemon(String.format("kafka-kerberos-refresh-thread-%s", principal), () -> {
            log.info("[Principal={}]: TGT refresh thread started, minTimeBeforeRelogin = {}", principal, minTimeBeforeRelogin);
            while (true) {
                try {
                    Thread.sleep(minTimeBeforeRelogin);
                } catch (InterruptedException ie) {
                    log.warn("[Principal={}]: TGT renewal thread has been interrupted and will exit.", principal);
                    return;
                }
                try {
                    checkTGTAndReLogin();
                } catch (Throwable t) {
                    log.error("[Principal={}]: Error from TGT refresh thread", principal, t);
                }
            }
        });
        refreshThread.start();
        return loginContext;
    }

    @Override
    public void close() {
        if ((refreshThread != null) && (refreshThread.isAlive())) {
            refreshThread.interrupt();
            try {
                refreshThread.join();
            } catch (InterruptedException e) {
                log.warn("[Principal={}]: Error while waiting for Login thread to shutdown.", principal, e);
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public Subject subject() {
        return subject;
    }

    @Override
    public String serviceName() {
        return serviceName;
    }

    private synchronized void checkTGTAndReLogin() throws LoginException {
        final KerberosTicket tgt = getTGT();
        if (tgt == null) {
            log.info("[Principal={}]: TGT was not found, performing login", principal);
            reLogin();
            return;
        }

        if (System.currentTimeMillis() < getRefreshTime(tgt)) {
            log.debug("[Principal={}]: TGT was found, but has not reached expiration window", principal);
            return;
        }

        try {
            tgt.refresh();
            log.info("[Principal={}]: TGT refreshed", principal);
            getRefreshTime(tgt);
        } catch (RefreshFailedException rfe) {
            log.warn("[Principal={}]: TGT refresh failed, will attempt relogin", principal);
            log.debug("", rfe);
            reLogin();
        }
    }

    private static String getServiceName(Map<String, ?> configs, String contextName, Configuration configuration) {
        List<AppConfigurationEntry> configEntries = Arrays.asList(configuration.getAppConfigurationEntry(contextName));
        String jaasServiceName = JaasContext.configEntryOption(configEntries, JaasUtils.SERVICE_NAME, null);
        String configServiceName = (String) configs.get(SaslConfigs.SASL_KERBEROS_SERVICE_NAME);
        if (jaasServiceName != null && configServiceName != null && !jaasServiceName.equals(configServiceName)) {
            String message = String.format("Conflicting serviceName values found in JAAS and Kafka configs " +
                    "value in JAAS file %s, value in Kafka config %s", jaasServiceName, configServiceName);
            throw new IllegalArgumentException(message);
        }

        if (jaasServiceName != null) {
            return jaasServiceName;
        }
        if (configServiceName != null) {
            return configServiceName;
        }

        throw new IllegalArgumentException("No serviceName defined in either JAAS or Kafka config");
    }


    private long getRefreshTime(final KerberosTicket tgt) {
        long start = tgt.getStartTime().getTime();
        long expires = tgt.getEndTime().getTime();

        log.debug("[Principal={}]: TGT valid starting at: {}", principal, tgt.getStartTime());
        log.debug("[Principal={}]: TGT expires: {}", principal, tgt.getEndTime());
        log.debug("[Principal={}]: TGT renew until: {}", principal, tgt.getRenewTill());

        return start + (long) ((expires - start) * ticketRenewWindowFactor);
    }

    private KerberosTicket getTGT() {
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        for (KerberosTicket ticket : tickets) {
            KerberosPrincipal server = ticket.getServer();
            final String expectedServerName = String.format("krbtgt/%s@%s", server.getRealm(), server.getRealm());
            if (server.getName().equals(expectedServerName)) {
                log.debug("Found TGT with client principal '{}' and server principal '{}'.", ticket.getClient().getName(),
                        ticket.getServer().getName());
                return ticket;
            }
        }
        return null;
    }

    /**
     * Re-login a principal. This method assumes that {@link #login()} has happened already.
     * @throws javax.security.auth.login.LoginException on a failure
     */
    private void reLogin() throws LoginException {
        if (!isKrbTicket) {
            return;
        }
        if (loginContext == null) {
            throw new LoginException("Login must be done first");
        }

        synchronized (KerberosLogin.class) {
            log.info("Initiating logout for {}", principal);
            //clear up the kerberos state. But the tokens are not cleared! As per
            //the Java kerberos login module code, only the kerberos credentials
            //are cleared
            loginContext.logout();
            //login and also update the subject field of this instance to
            //have the new credentials (pass it to the LoginContext constructor)
            loginContext = new LoginContext(contextName(), subject, null, configuration());
            log.info("Initiating re-login for {}", principal);
            loginContext.login();
            log.info("Successful re-login for {}", principal);
        }
    }

}
