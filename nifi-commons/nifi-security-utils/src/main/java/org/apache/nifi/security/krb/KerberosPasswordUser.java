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

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.HashMap;

/**
 * KerberosUser that authenticates via username and password instead of keytab.
 */
public class KerberosPasswordUser extends AbstractKerberosUser {

    private final String password;

    public KerberosPasswordUser(final String principal, final String password) {
        super(principal);
        this.password = password;
        Validate.notBlank(this.password);
    }

    @Override
    protected LoginContext createLoginContext(final Subject subject) throws LoginException {
        final Configuration configuration = new PasswordConfig();
        final CallbackHandler callbackHandler = new UsernamePasswordCallbackHandler(principal, password);
        return new LoginContext("PasswordConf", subject, callbackHandler, configuration);
    }

    /**
     * JAAS Configuration to use when logging in with username/password.
     */
    private static class PasswordConfig extends Configuration {

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            HashMap<String, String> options = new HashMap<String, String>();
            options.put("storeKey", "true");
            options.put("refreshKrb5Config", "true");

            final String krbLoginModuleName = ConfigurationUtil.IS_IBM
                    ? ConfigurationUtil.IBM_KRB5_LOGIN_MODULE : ConfigurationUtil.SUN_KRB5_LOGIN_MODULE;

            return new AppConfigurationEntry[] {
                    new AppConfigurationEntry(
                            krbLoginModuleName,
                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                            options
                    )
            };
        }

    }

    /**
     * CallbackHandler that provides the given username and password.
     */
    private static class UsernamePasswordCallbackHandler implements CallbackHandler {

        private final String username;
        private final String password;

        public UsernamePasswordCallbackHandler(final String username, final String password) {
            this.username = username;
            this.password = password;
            Validate.notBlank(this.username);
            Validate.notBlank(this.password);
        }

        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (final Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    final NameCallback nameCallback = (NameCallback) callback;
                    nameCallback.setName(username);
                } else if (callback instanceof PasswordCallback) {
                    final PasswordCallback passwordCallback = (PasswordCallback) callback;
                    passwordCallback.setPassword(password.toCharArray());
                } else {
                    throw new IllegalStateException("Unexpected callback type: " + callback.getClass().getCanonicalName());
                }
            }
        }

    }

}
