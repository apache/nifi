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

import java.io.IOException;
import java.util.Random;

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
     * {@link UserGroupInformation} instance.  Setting <code>loginUser</code> is necessary for
     * {@link org.apache.hadoop.ipc.Client.Connection#handleSaslConnectionFailure(int, int, Exception, Random, UserGroupInformation)}
     * to be able to attempt a relogin during a connection failure.  The <code>handleSaslConnectionFailure</code> method
     * calls <code>UserGroupInformation.getLoginUser().reloginFromKeytab()</code> statically, which can return null
     * if <code>loginUser</code> is not set, resulting in failure of the hadoop operation.
     * <p/>
     * In previous versions of NiFi, {@link UserGroupInformation#loginUserFromKeytabAndReturnUGI(String, String)} was
     * used to authenticate the <code>principal</code>, which does not set <code>loginUser</code>, making it impossible
     * for a
     * {@link org.apache.hadoop.ipc.Client.Connection#handleSaslConnectionFailure(int, int, Exception, Random, UserGroupInformation)}
     * to be able to implicitly relogin the principal.
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
        Validate.notNull(config);
        Validate.notNull(principal);
        Validate.notNull(keyTab);

        UserGroupInformation.setConfiguration(config);
        UserGroupInformation.loginUserFromKeytab(principal.trim(), keyTab.trim());
        return UserGroupInformation.getCurrentUser();
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
        Validate.notNull(config);
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
        Validate.notNull(config);
        return KERBEROS.equalsIgnoreCase(config.get(HADOOP_SECURITY_AUTHENTICATION));
    }
}
