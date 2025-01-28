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
package org.apache.nifi.hadoop;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Provides synchronized access to UserGroupInformation to avoid multiple processors/services from
 * interfering with each other.
 */
public class SecurityUtil {
    public static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    public static final String KERBEROS = "kerberos";

    /**
     * Initializes UserGroupInformation with the given Configuration and performs the login for the given principal
     * and keytab. All logins should happen through this class to ensure other threads are not concurrently modifying
     * UserGroupInformation.
     * <p/>
     * As of Apache NiFi 1.5.0, this method uses {@link UserGroupInformation#loginUserFromKeytab(String, String)} to
     * authenticate the given <code>principal</code>, which sets the static variable <code>loginUser</code> in the
     * {@link UserGroupInformation} instance.
     * calls <code>UserGroupInformation.getLoginUser().reloginFromKeytab()</code> statically, which can return null
     * if <code>loginUser</code> is not set, resulting in failure of the hadoop operation.
     * <p/>
     *
     * @param config the configuration instance
     * @param principal the principal to authenticate as
     * @param keyTab the keytab to authenticate with
     *
     * @return the UGI for the given principal
     *
     * @throws IOException if login failed
     */
    public static synchronized UserGroupInformation loginKerberos(final Configuration config, final String principal, final String keyTab)
            throws IOException {
        Objects.requireNonNull(config);
        Objects.requireNonNull(principal);
        Objects.requireNonNull(keyTab);

        UserGroupInformation.setConfiguration(config);
        UserGroupInformation.loginUserFromKeytab(principal.trim(), keyTab.trim());
        return UserGroupInformation.getCurrentUser();
    }

    /**
     * Authenticates a {@link KerberosUser} and acquires a {@link UserGroupInformation} instance using {@link UserGroupInformation#getUGIFromSubject(Subject)}.
     * The {@link UserGroupInformation} will use the given {@link Configuration}.
     *
     * @param config The Configuration to apply to the acquired UserGroupInformation instance
     * @param kerberosUser The KerberosUser to authenticate
     * @return A UserGroupInformation instance created using the Subject of the given KerberosUser
     * @throws IOException if authentication fails
     */
    public static synchronized UserGroupInformation getUgiForKerberosUser(final Configuration config, final KerberosUser kerberosUser) throws IOException {
        UserGroupInformation.setConfiguration(config);
        try {
            if (kerberosUser.isLoggedIn()) {
                kerberosUser.checkTGTAndRelogin();
            } else {
                kerberosUser.login();
            }
            return kerberosUser.doAs((PrivilegedExceptionAction<UserGroupInformation>) () -> {
                Subject subject = Subject.current();
                Validate.notEmpty(
                        subject.getPrincipals(KerberosPrincipal.class).stream().filter(p -> p.getName().startsWith(kerberosUser.getPrincipal())).collect(Collectors.toSet()),
                        "No Subject was found matching the given principal");

                // getUGIFromSubject does not set the static logged in user inside UGI and some Hadoop client code
                // depends on this so we have to make sure to set it ourselves
                final UserGroupInformation ugi = UserGroupInformation.getUGIFromSubject(subject);
                UserGroupInformation.setLoginUser(ugi);
                return ugi;
            });
        } catch (final PrivilegedActionException e) {
            throw new IOException("Unable to acquire UGI for KerberosUser: " + e.getException().getLocalizedMessage(), e.getException());
        } catch (final KerberosLoginException e) {
            throw new IOException("Unable to acquire UGI for KerberosUser: " + e.getLocalizedMessage(), e);
        }
    }

    /**
     * Initializes UserGroupInformation with the given Configuration and returns UserGroupInformation.getLoginUser().
     * All logins should happen through this class to ensure other threads are not concurrently modifying
     * UserGroupInformation.
     *
     * @param config the configuration instance
     *
     * @return the UGI for the given principal
     *
     * @throws IOException if login failed
     */
    public static synchronized UserGroupInformation loginSimple(final Configuration config) throws IOException {
        Objects.requireNonNull(config);
        UserGroupInformation.setConfiguration(config);
        return UserGroupInformation.getLoginUser();
    }

    /**
     * Initializes UserGroupInformation with the given Configuration and returns UserGroupInformation.isSecurityEnabled().
     *
     * All checks for isSecurityEnabled() should happen through this method.
     *
     * @param config the given configuration
     *
     * @return true if kerberos is enabled on the given configuration, false otherwise
     *
     */
    public static boolean isSecurityEnabled(final Configuration config) {
        Objects.requireNonNull(config);
        return KERBEROS.equalsIgnoreCase(config.get(HADOOP_SECURITY_AUTHENTICATION));
    }

    /**
     * Helper method to execute the given action as the given user.
     *
     * @param ugi the user
     * @param action the action
     * @param <T> the result type of the action
     * @return the result of the action
     * @throws IOException if the action was interrupted
     */
    public static <T> T callWithUgi(final UserGroupInformation ugi, final PrivilegedExceptionAction<T> action) throws IOException {
        try {
            T result;
            if (ugi == null) {
                try {
                    result = action.run();
                } catch (IOException | RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }  else {
                result = ugi.doAs(action);
            }
            return result;
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    /**
     * Helper method to call checkTGTAndRelogin on a given KerberosUser that may be null.
     *
     * @param log the logger
     * @param kerberosUser the kerberos user
     * @throws KerberosLoginException if an error occurs when checkTGTAndRelogin calls login or logout
     */
    public static void checkTGTAndRelogin(final ComponentLog log, final KerberosUser kerberosUser) {
        if (kerberosUser == null) {
            log.debug("kerberosUser was null, will not refresh TGT with KerberosUser");
            return;
        }

        log.debug("checking TGT on kerberosUser {}", kerberosUser);
        kerberosUser.checkTGTAndRelogin();
    }

}
