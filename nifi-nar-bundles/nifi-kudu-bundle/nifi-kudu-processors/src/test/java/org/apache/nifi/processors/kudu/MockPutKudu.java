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

import org.apache.kudu.Schema;
import org.apache.kudu.client.DeleteIgnore;
import org.apache.kudu.client.InsertIgnore;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.UpdateIgnore;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.client.Update;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.record.Record;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockPutKudu extends PutKudu {

    private KuduSession session;
    private LinkedList<Operation> opQueue;

    // Atomic reference is used as the set and use of the schema are in different thread
    private AtomicReference<Schema> tableSchema = new AtomicReference<>();

    private boolean loggedIn = false;
    private boolean loggedOut = false;

    public MockPutKudu() {
        this(mock(KuduSession.class));
    }

    public MockPutKudu(KuduSession session) {
        this.session = session;
        this.opQueue = new LinkedList<>();
    }

    public void queue(Operation... operations) {
        opQueue.addAll(Arrays.asList(operations));
    }

    @Override
    protected Operation createKuduOperation(OperationType operationType, Record record,
                                            List<String> fieldNames, boolean ignoreNull,
                                            boolean lowercaseFields, KuduTable kuduTable) {
        Operation operation = opQueue.poll();
        if (operation == null) {
            switch (operationType) {
                case INSERT:
                    operation = mock(Insert.class);
                    break;
                case INSERT_IGNORE:
                    operation = mock(InsertIgnore.class);
                    break;
                case UPSERT:
                    operation = mock(Upsert.class);
                    break;
                case UPDATE:
                    operation = mock(Update.class);
                    break;
                case UPDATE_IGNORE:
                    operation = mock(UpdateIgnore.class);
                    break;
                case DELETE:
                    operation = mock(Delete.class);
                    break;
                case DELETE_IGNORE:
                    operation = mock(DeleteIgnore.class);
                    break;
                default:
                    throw new IllegalArgumentException(String.format("OperationType: %s not supported by Kudu", operationType));
            }
        }
        return operation;
    }

    @Override
    protected boolean supportsIgnoreOperations() {
        return true;
    }

    @Override
    public KuduClient buildClient(ProcessContext context) {
        final KuduClient client = mock(KuduClient.class);

        try {
            when(client.openTable(anyString())).thenReturn(mock(KuduTable.class));
        } catch (final Exception e) {
            throw new AssertionError(e);
        }

        return client;
    }

    @Override
    protected void executeOnKuduClient(Consumer<KuduClient> actionOnKuduClient) {
        final KuduClient client = mock(KuduClient.class);

        try {
            final KuduTable kuduTable = mock(KuduTable.class);
            when(client.openTable(anyString())).thenReturn(kuduTable);
            when(kuduTable.getSchema()).thenReturn(tableSchema.get());
        } catch (final Exception e) {
            throw new AssertionError(e);
        }

        actionOnKuduClient.accept(client);
    }

    public boolean loggedIn() {
        return loggedIn;
    }

    public boolean loggedOut() {
        return loggedOut;
    }

    @Override
    protected KerberosUser createKerberosKeytabUser(String principal, String keytab, ProcessContext context) {
        return createMockKerberosUser(principal);
    }

    @Override
    protected KerberosUser createKerberosPasswordUser(String principal, String password, ProcessContext context) {
        return createMockKerberosUser(principal);
    }

    private KerberosUser createMockKerberosUser(final String principal) {
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
    protected KuduSession createKuduSession(final KuduClient client) {
        return session;
    }

    void setTableSchema(final Schema tableSchema) {
        this.tableSchema.set(tableSchema);
    }
}