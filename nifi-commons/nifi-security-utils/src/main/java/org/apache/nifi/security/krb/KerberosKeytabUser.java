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
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Used to authenticate and execute actions when Kerberos is enabled and a keytab is being used.
 *
 * Some of the functionality in this class is adapted from Hadoop's UserGroupInformation.
 */
public class KerberosKeytabUser extends AbstractKerberosUser {

    private final String keytabFile;

    public KerberosKeytabUser(final String principal, final String keytabFile) {
        super(principal);
        this.keytabFile = keytabFile;
        Validate.notBlank(keytabFile);
    }

    @Override
    protected LoginContext createLoginContext(Subject subject) throws LoginException {
        final Configuration config = new KeytabConfiguration(principal, keytabFile);
        return new LoginContext("KeytabConf", subject, null, config);
    }

    /**
     * @return the keytab file for this user
     */
    public String getKeytabFile() {
        return keytabFile;
    }

    // Visible for testing
    Subject getSubject() {
        return this.subject;
    }

}
