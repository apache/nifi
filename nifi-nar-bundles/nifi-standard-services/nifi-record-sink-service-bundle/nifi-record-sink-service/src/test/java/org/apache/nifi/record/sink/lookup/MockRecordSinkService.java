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
package org.apache.nifi.record.sink.lookup;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.record.sink.RetryableIOException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockRecordSinkService extends AbstractConfigurableComponent implements RecordSinkService {

    private List<Map<String, Object>> rows = new ArrayList<>();
    private boolean transmitted = false;
    private boolean failWithRetryableError = false;
    private String name;
    private int resetCount = 0;

    public MockRecordSinkService(String name) {
        this.name = name;
    }

    public MockRecordSinkService(String name, boolean failWithRetryableError) {
        this(name);
        this.failWithRetryableError = failWithRetryableError;
    }

    @Override
    public WriteResult sendData(RecordSet recordSet, Map<String, String> attributes, boolean sendZeroResults) throws IOException {
        if (failWithRetryableError) {
            throw new RetryableIOException("Retryable");
        }
        int numRecordsWritten = 0;
        RecordSchema recordSchema = recordSet.getSchema();
        Record record;
        while ((record = recordSet.next()) != null) {
            Map<String, Object> row = new HashMap<>();
            final Record finalRecord = record;
            recordSchema.getFieldNames().forEach((fieldName) -> row.put(fieldName, finalRecord.getValue(fieldName)));
            rows.add(row);
            numRecordsWritten++;
        }

        if (numRecordsWritten > 0 || sendZeroResults) {
            transmitted = true;
        }

        Map<String, String> returnAttributes = new HashMap<>(attributes);
        returnAttributes.put("my.name", name);
        return WriteResult.of(numRecordsWritten, returnAttributes);
    }

    @Override
    public void reset() {
        resetCount++;
    }

    @Override
    public String getIdentifier() {
        return name;
    }

    @Override
    public void initialize(ControllerServiceInitializationContext context) throws InitializationException {
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public boolean isTransmitted() {
        return transmitted;
    }

    public int getResetCount() {
        return resetCount;
    }

    public void setFailWithRetryableError(boolean failWithRetryableError) {
        this.failWithRetryableError = failWithRetryableError;
    }
}
