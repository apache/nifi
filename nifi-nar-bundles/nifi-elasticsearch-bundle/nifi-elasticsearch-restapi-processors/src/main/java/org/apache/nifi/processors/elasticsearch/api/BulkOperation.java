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

package org.apache.nifi.processors.elasticsearch.api;

import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.List;

public class BulkOperation {
    private List<IndexOperationRequest> operationList;
    private List<Record> originalRecords;
    private RecordSchema schema;

    public BulkOperation(List<IndexOperationRequest> operationList, List<Record> originalRecords, RecordSchema schema) {
        this.operationList = operationList;
        this.originalRecords = originalRecords;
        this.schema = schema;
    }

    public List<IndexOperationRequest> getOperationList() {
        return operationList;
    }

    public List<Record> getOriginalRecords() {
        return originalRecords;
    }

    public RecordSchema getSchema() {
        return schema;
    }
}