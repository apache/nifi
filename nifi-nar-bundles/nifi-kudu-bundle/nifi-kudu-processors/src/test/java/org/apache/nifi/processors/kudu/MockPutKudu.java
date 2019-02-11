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

package org.apache.nifi.processors.kudu;

import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Upsert;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.record.Record;

import javax.security.auth.login.LoginException;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockPutKudu extends PutKudu {
    private KuduSession session;
    private LinkedList<Insert> insertQueue;

    private boolean loggedIn = false;
    private boolean loggedOut = false;

    public MockPutKudu() {
        this(mock(KuduSession.class));
    }

    public MockPutKudu(KuduSession session) {
        this.session = session;
        this.insertQueue = new LinkedList<>();
    }

    public void queue(Insert... operations) {
        insertQueue.addAll(Arrays.asList(operations));
    }

    @Override
    protected Insert insertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames) {
        Insert insert = insertQueue.poll();
        return insert != null ? insert : mock(Insert.class);
    }

    @Override
    protected Upsert upsertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames) {
        return mock(Upsert.class);
    }

    @Override
    protected KuduClient buildClient(final String masters) {
        final KuduClient client = mock(KuduClient.class);

        try {
            when(client.openTable(anyString())).thenReturn(mock(KuduTable.class));
        } catch (final Exception e) {
            throw new AssertionError(e);
        }

        return client;
    }

    public boolean loggedIn() {
        return loggedIn;
    }

    public boolean loggedOut() {
        return loggedOut;
    }

    @Override
    protected KerberosUser loginKerberosUser(final String principal, final String keytab) throws LoginException {
        return new KerberosUser() {

            @Override
            public void login() {
                loggedIn = true;
            }

            @Override
            public void logout() {
                loggedOut = true;
            }

            @Override
            public <T> T doAs(final PrivilegedAction<T> action) throws IllegalStateException {
                return action.run();
            }

            @Override
            public <T> T doAs(final PrivilegedExceptionAction<T> action) throws IllegalStateException, PrivilegedActionException {
                try {
                    return action.run();
                } catch (Exception e) {
                    throw new PrivilegedActionException(e);
                }
            }

            @Override
            public boolean checkTGTAndRelogin() {
                return true;
            }

            @Override
            public boolean isLoggedIn() {
                return loggedIn && !loggedOut;
            }

            @Override
            public String getPrincipal() {
                return principal;
            }
        };
    }

    @Override
    protected KuduSession getKuduSession(KuduClient client) {
        return session;
    }
}
