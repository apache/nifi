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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

public class ReentrantKerberosUser implements KerberosUser {
    private static final Logger logger = LoggerFactory.getLogger(ReentrantKerberosUser.class);

    private final KerberosUser delegate;
    private long loginCount = 0L;

    public ReentrantKerberosUser(final KerberosUser delegate) {
        this.delegate = delegate;
    }

    @Override
    public synchronized void login() {
        loginCount++;

        if (loginCount == 1) {
            logger.debug("{} KerberosUser login count is now 1, delegating login call to underlying KerberosUser {}", this, delegate);
            delegate.login();
        } else {
            logger.debug("{} KerberosUser login count is now {}, will not delegate login call to underlying KerberosUser {}", this, loginCount, delegate);
        }
    }

    @Override
    public synchronized void logout() {
        if (!isLoggedIn()) {
            logger.debug("{} KerberosUser logout was called, but not currently logged in, will not decrement login count or delegate logout call to underlying KerberosUser {}", this, delegate);
            return;
        }

        loginCount--;
        if (loginCount < 1) {
            logger.debug("{} KerberosUser login count is now {}, delegating logout call to underlying KerberosUser {}", this, loginCount, delegate);
            delegate.logout();
        } else {
            logger.debug("{} KerberosUser login count is now {}, will not delegate logout call to underlying KerberosUser {}", this, loginCount, delegate);
        }
    }

    @Override
    public <T> T doAs(final PrivilegedAction<T> action) throws IllegalStateException {
        return delegate.doAs(action);
    }

    @Override
    public <T> T doAs(final PrivilegedExceptionAction<T> action) throws IllegalStateException, PrivilegedActionException {
        return delegate.doAs(action);
    }

    @Override
    public boolean checkTGTAndRelogin() {
        return delegate.checkTGTAndRelogin();
    }

    @Override
    public boolean isLoggedIn() {
        return delegate.isLoggedIn();
    }

    @Override
    public String getPrincipal() {
        return delegate.getPrincipal();
    }

    @Override
    public AppConfigurationEntry getConfigurationEntry() {
        return delegate.getConfigurationEntry();
    }
}
