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

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;

/**
 * KerberosUser that authenticates via username and password instead of keytab.
 */
public class KerberosPasswordUser extends AbstractKerberosUser {

    private final String password;

    public KerberosPasswordUser(final String principal, final String password) {
        super(principal);
        this.password = Validate.notBlank(password);
    }

    @Override
    protected Configuration createConfiguration() {
        return new PasswordConfiguration();
    }

    @Override
    protected CallbackHandler createCallbackHandler() {
        return new UsernamePasswordCallbackHandler(principal, password);
    }

}
