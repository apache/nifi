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

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.Upsert;

import org.apache.nifi.serialization.record.Record;

import java.util.List;

import static org.mockito.Mockito.mock;

public class MockPutKudu extends PutKudu{
  @Override
  protected Insert insertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames) {
    return mock(Insert.class);
  }

  @Override
  protected Upsert upsertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames) {
    return mock(Upsert.class);
  }

  @Override
  protected KuduClient getKuduConnection(String masters) {
    return mock(KuduClient.class);
  }

  @Override
  protected KuduSession getKuduSession(KuduClient client){
    return mock(KuduSession.class);
  }

  @Override
  protected KuduTable getKuduTable(KuduClient client, String tableName) throws KuduException {
    return mock(KuduTable.class);
  }
}
