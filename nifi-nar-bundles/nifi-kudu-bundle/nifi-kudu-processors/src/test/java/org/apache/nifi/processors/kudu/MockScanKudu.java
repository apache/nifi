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

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.Insert;
import org.apache.nifi.processors.kudu.io.ResultHandler;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.security.krb.KerberosUser;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockScanKudu extends AbstractMockKuduProcessor {

    private LinkedList<Insert> insertQueue;
    private int numScans = 0;
    private boolean throwException = false;
    private int linesBeforeException = -1;
    RowResultIterator testRows = mock(RowResultIterator.class);
    List<RowResult> rowResultList = new ArrayList<>();

    public MockScanKudu() {
        this.insertQueue = new LinkedList<>();
    }

    public void queue(Insert... operations) {
        insertQueue.addAll(Arrays.asList(operations));
    }

    @Override
    public void scan(ProcessContext context, ProcessSession session, KuduTable kuduTable, String predicates, List<String> projectedColumnNames, ResultHandler handler) throws IOException {
        if (throwException) {
            throw new IOException("exception");
        }

        List<RowResult> matchedRows = new ArrayList<>();
        Iterator<RowResult> it = testRows.iterator();
        if (it != null) {
            if (predicates == null || predicates.isEmpty() || !predicates.contains("=")) {
                while (it.hasNext()) {
                    matchedRows.add(it.next());
                    handler.handle(it.next());
                }
            } else {
                if (predicates.contains("=")) {
                    final String[] parts = predicates.split("=");
                    int i = 0;
                    while (it.hasNext()) {
                        if (linesBeforeException >= 0 && i++ >= linesBeforeException) {
                            throw new IOException("iterating exception");
                        }
                        RowResult result = it.next();
                        if (parts[1].equals(result.getString(String.valueOf(parts[0])))) {
                            matchedRows.add(result);
                            handler.handle(result);
                        }
                    }
                }
            }
        }
        numScans++;
    }

    public void addResult(Map<String, String> rows) {
        rows.entrySet().forEach(kv -> rowResultList.add(createRowResult(kv.getKey(), kv.getValue())));
        when(testRows.iterator()).thenReturn(rowResultList.iterator());
        when(testRows.getNumRows()).thenReturn(rowResultList.size());
    }

    public static RowResult createRowResult(String key, String value) {
        RowResult row = mock(RowResult.class);

        List<ColumnSchema> colSchema = new ArrayList<>();
        colSchema.add(new ColumnSchema.ColumnSchemaBuilder(key, Type.STRING).build());

        when(row.getString(key)).thenReturn(value);
        when(row.getSchema()).thenReturn(new Schema(colSchema));
        return row;
    }


    public int getNumScans() {
        return numScans;
    }

    public void setThrowException(boolean throwException) {
        this.throwException = throwException;
    }

    public void setLinesBeforeException(int linesBeforeException) {
        this.linesBeforeException = linesBeforeException;
    }
}
