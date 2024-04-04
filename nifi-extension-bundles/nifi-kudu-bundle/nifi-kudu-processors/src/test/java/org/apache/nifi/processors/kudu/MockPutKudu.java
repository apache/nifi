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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.kudu.Schema;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.DeleteIgnore;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.InsertIgnore;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.Update;
import org.apache.kudu.client.UpdateIgnore;
import org.apache.kudu.client.Upsert;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.serialization.record.Record;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockPutKudu extends PutKudu {

    private final KuduSession session;
    private final LinkedList<Operation> opQueue;

    // Atomic reference is used as the set and use of the schema are in different thread
    private final AtomicReference<Schema> tableSchema = new AtomicReference<>();

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
            operation = switch (operationType) {
                case INSERT -> mock(Insert.class);
                case INSERT_IGNORE -> mock(InsertIgnore.class);
                case UPSERT -> mock(Upsert.class);
                case UPDATE -> mock(Update.class);
                case UPDATE_IGNORE -> mock(UpdateIgnore.class);
                case DELETE -> mock(Delete.class);
                case DELETE_IGNORE -> mock(DeleteIgnore.class);
            };
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


    @Override
    protected KuduSession createKuduSession(final KuduClient client) {
        return session;
    }

    void setTableSchema(final Schema tableSchema) {
        this.tableSchema.set(tableSchema);
    }
}