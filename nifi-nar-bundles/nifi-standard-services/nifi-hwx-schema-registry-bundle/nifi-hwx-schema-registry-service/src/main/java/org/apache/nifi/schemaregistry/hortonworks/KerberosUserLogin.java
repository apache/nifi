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
package org.apache.nifi.schemaregistry.hortonworks;

import com.hortonworks.registries.auth.Login;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosUser;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

/**
 * Implementation of Schema Registry's Login interface that wraps NiFi's KerberosUser API.
 */
public class KerberosUserLogin implements Login {

    private final KerberosUser kerberosUser;
    private final ComponentLog logger;

    public KerberosUserLogin(final KerberosUser kerberosUser, final ComponentLog logger) {
        this.kerberosUser = kerberosUser;
        this.logger = logger;
    }

    @Override
    public void configure(Map<String, ?> configs, String loginContextName) {

    }

    @Override
    public LoginContext login() throws LoginException {
        kerberosUser.login();

        // the KerberosUser doesn't expose the LoginContext, but SchemaRegistryClient doesn't use
        // the returned context at all, so we just return null here
        return null;
    }

    @Override
    public <T> T doAction(PrivilegedAction<T> action) throws LoginException {
        final PrivilegedExceptionAction<T> wrappedAction = () -> action.run();
        final KerberosAction<T> kerberosAction = new KerberosAction<T>(kerberosUser, wrappedAction, logger);
        return kerberosAction.execute();
    }

    @Override
    public void close() {

    }

}
