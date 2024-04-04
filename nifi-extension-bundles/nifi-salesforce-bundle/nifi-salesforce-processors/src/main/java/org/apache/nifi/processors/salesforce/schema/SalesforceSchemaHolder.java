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
package org.apache.nifi.processors.salesforce.schema;

import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.nifi.serialization.record.RecordSchema;

public class SalesforceSchemaHolder {

    RecordSchema querySObjectResultSchema;
    RecordSchema recordSchema;
    SObjectDescription salesforceObject;

    public SalesforceSchemaHolder(RecordSchema querySObjectResultSchema, RecordSchema recordSchema, SObjectDescription salesforceObject) {
        this.querySObjectResultSchema = querySObjectResultSchema;
        this.recordSchema = recordSchema;
        this.salesforceObject = salesforceObject;
    }

    public SObjectDescription getSalesforceObject() {
        return salesforceObject;
    }

    public RecordSchema getQuerySObjectResultSchema() {
        return querySObjectResultSchema;
    }

    public RecordSchema getRecordSchema() {
        return recordSchema;
    }
}
