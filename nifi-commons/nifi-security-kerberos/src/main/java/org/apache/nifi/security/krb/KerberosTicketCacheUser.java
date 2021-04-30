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

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Used to perform actions as a Subject that already has a ticket in the ticket cache.
 */
public class KerberosTicketCacheUser extends AbstractKerberosUser {

    public KerberosTicketCacheUser(final String principal) {
        super(principal);
    }

    @Override
    protected LoginContext createLoginContext(Subject subject) throws LoginException {
        final Configuration config = new TicketCacheConfiguration(principal);
        return new LoginContext("TicketCacheConf", subject, null, config);
    }

    // Visible for testing
    Subject getSubject() {
        return this.subject;
    }
}
