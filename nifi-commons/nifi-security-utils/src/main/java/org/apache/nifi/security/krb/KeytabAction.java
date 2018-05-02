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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;

import javax.security.auth.login.LoginException;
import java.security.PrivilegedAction;

/**
 * Helper class for processors to perform an action as a KeytabUser.
 */
public class KeytabAction {

    private final KeytabUser keytabUser;
    private final PrivilegedAction action;
    private final ProcessContext context;
    private final ComponentLog logger;

    public KeytabAction(final KeytabUser keytabUser,
                        final PrivilegedAction action,
                        final ProcessContext context,
                        final ComponentLog logger) {
        this.keytabUser = keytabUser;
        this.action = action;
        this.context = context;
        this.logger = logger;
        Validate.notNull(this.keytabUser);
        Validate.notNull(this.action);
        Validate.notNull(this.context);
        Validate.notNull(this.logger);
    }

    public void execute() {
        // lazily login the first time the processor executes
        if (!keytabUser.isLoggedIn()) {
            try {
                keytabUser.login();
                logger.info("Successful login for {}", new Object[]{keytabUser.getPrincipal()});
            } catch (LoginException e) {
                // make sure to yield so the processor doesn't keep retrying the rolled back flow files immediately
                context.yield();
                throw new ProcessException("Login failed due to: " + e.getMessage(), e);
            }
        }

        // check if we need to re-login, will only happen if re-login window is reached (80% of TGT life)
        try {
            keytabUser.checkTGTAndRelogin();
        } catch (LoginException e) {
            // make sure to yield so the processor doesn't keep retrying the rolled back flow files immediately
            context.yield();
            throw new ProcessException("Relogin check failed due to: " + e.getMessage(), e);
        }

        // attempt to execute the action, if an exception is caught attempt to logout/login and retry
        try {
            keytabUser.doAs(action);
        } catch (SecurityException se) {
            logger.info("Privileged action failed, attempting relogin and retrying...");
            logger.debug("", se);

            try {
                keytabUser.logout();
                keytabUser.login();
                keytabUser.doAs(action);
            } catch (Exception e) {
                // make sure to yield so the processor doesn't keep retrying the rolled back flow files immediately
                context.yield();
                throw new ProcessException("Retrying privileged action failed due to: " + e.getMessage(), e);
            }
        }
    }

}
