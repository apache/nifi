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

import java.util.Collections;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Insert;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;


public class MockScanKudu extends AbstractMockKuduProcessor {
    public KuduTable getKuduTable(String tableName, List<ColumnSchema> columns) throws KuduException {
        Schema schema = new Schema(columns);
        CreateTableOptions opts = new CreateTableOptions().setRangePartitionColumns(Collections.singletonList("key"));
        System.out.println(this.kuduClient);
        if(!this.kuduClient.tableExists(tableName)) {
            this.kuduClient.createTable(tableName, schema, opts);
        }
        return kuduClient.openTable(tableName);
    }

    public void insertTestRecordsToKuduTable(String table, Map<String, String> data) throws KuduException {
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key1", Type.STRING).build());

        KuduTable kuduTable = this.getKuduTable(table, columns);
        Insert insert = kuduTable.newInsert();
        PartialRow row = insert.getRow();
        for(Map.Entry<String, String> entry: data.entrySet()) {
            row.addString(entry.getKey(), entry.getValue());
        }
        KuduSession kuduSession = this.kuduClient.newSession();
        kuduSession.apply(insert);
        kuduSession.close();
    }
}
