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
package org.apache.nifi.hbase;

import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;

public class HBaseTestUtil {

    public static void verifyPut(final String row, final String columnFamily, final Map<String,byte[]> columns, final List<PutFlowFile> puts) {
        verifyPut(row, columnFamily, null, columns, puts);
    }

    public static void verifyPut(final String row, final String columnFamily, final Long timestamp, final Map<String,byte[]> columns, final List<PutFlowFile> puts) {
        boolean foundPut = false;

        for (final PutFlowFile put : puts) {
            if (!row.equals(new String(put.getRow(), StandardCharsets.UTF_8))) {
                continue;
            }

            if (put.getColumns() == null || put.getColumns().size() != columns.size()) {
                continue;
            }

            // start off assuming we have all the columns
            boolean foundAllColumns = true;

            for (Map.Entry<String, byte[]> entry : columns.entrySet()) {
                // determine if we have the current expected column
                boolean foundColumn = false;
                for (PutColumn putColumn : put.getColumns()) {
                    if (columnFamily.equals(new String(putColumn.getColumnFamily(), StandardCharsets.UTF_8))
                            && entry.getKey().equals(new String(putColumn.getColumnQualifier(), StandardCharsets.UTF_8))
                            && Arrays.equals(entry.getValue(), putColumn.getBuffer())
                            && ((timestamp == null && putColumn.getTimestamp() == null)
                                    || (timestamp != null && timestamp.equals(putColumn.getTimestamp())) )) {
                        foundColumn = true;
                        break;
                    }
                }

                // if we didn't have the current expected column we know we don't have all expected columns
                if (!foundColumn) {
                    foundAllColumns = false;
                    break;
                }
            }

            // if we found all the expected columns this was a match so we can break
            if (foundAllColumns) {
                foundPut = true;
                break;
            }
        }

        assertTrue(foundPut);
    }

    public static void verifyEvent(final List<ProvenanceEventRecord> events, final String uri, final ProvenanceEventType eventType) {
        boolean foundEvent = false;
        for (final ProvenanceEventRecord event : events) {
            if (event.getTransitUri().equals(uri) && event.getEventType().equals(eventType)) {
                foundEvent = true;
                break;
            }
        }
        assertTrue(foundEvent);
    }

    public static MockHBaseClientService getHBaseClientService(final TestRunner runner) throws InitializationException {
        final MockHBaseClientService hBaseClient = new MockHBaseClientService();
        runner.addControllerService("hbaseClient", hBaseClient);
        runner.enableControllerService(hBaseClient);
        runner.setProperty(PutHBaseCell.HBASE_CLIENT_SERVICE, "hbaseClient");
        return hBaseClient;
    }
}
