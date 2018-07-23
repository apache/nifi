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
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.KuduTable;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.serialization.record.Record;

import com.google.common.annotations.VisibleForTesting;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"put", "database", "NoSQL", "kudu", "HDFS", "record"})
@CapabilityDescription("Reads records from an incoming FlowFile using the provided Record Reader, and writes those records " +
        "to the specified Kudu's table. The schema for the table must be provided in the processor properties or from your source." +
        " If any error occurs while reading records from the input, or writing records to Kudu, the FlowFile will be routed to failure")
@WritesAttribute(attribute = "record.count", description = "Number of records written to Kudu")
public class PutKudu extends AbstractKudu {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(KUDU_MASTERS);
        properties.add(TABLE_NAME);
        properties.add(SKIP_HEAD_LINE);
        properties.add(RECORD_READER);
        properties.add(INSERT_OPERATION);
        properties.add(FLUSH_MODE);
        properties.add(BATCH_SIZE);

        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @Override
    protected Upsert upsertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames) throws IllegalStateException, Exception {
        Upsert upsert = kuduTable.newUpsert();
        this.buildPartialRow(kuduTable.getSchema(), upsert.getRow(), record, fieldNames);
        return upsert;
    }

    @Override
    protected Insert insertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames) throws IllegalStateException, Exception {
        Insert insert = kuduTable.newInsert();
        this.buildPartialRow(kuduTable.getSchema(), insert.getRow(), record, fieldNames);
        return insert;
    }

    @VisibleForTesting
    void buildPartialRow(Schema schema, PartialRow row, Record record, List<String> fieldNames) {
        for (String colName : fieldNames) {
            int colIdx = this.getColumnIndex(schema, colName);
            if (colIdx != -1) {
                ColumnSchema colSchema = schema.getColumnByIndex(colIdx);
                Type colType = colSchema.getType();

                if (record.getValue(colName) == null) {
                    row.setNull(colName);
                    continue;
                }

                switch (colType.getDataType(colSchema.getTypeAttributes())) {
                    case BOOL:
                        row.addBoolean(colIdx, record.getAsBoolean(colName));
                        break;
                    case FLOAT:
                        row.addFloat(colIdx, record.getAsFloat(colName));
                        break;
                    case DOUBLE:
                        row.addDouble(colIdx, record.getAsDouble(colName));
                        break;
                    case BINARY:
                        row.addBinary(colIdx, record.getAsString(colName).getBytes());
                        break;
                    case INT8:
                        row.addByte(colIdx, record.getAsInt(colName).byteValue());
                        break;
                    case INT16:
                        row.addShort(colIdx, record.getAsInt(colName).shortValue());
                        break;
                    case INT32:
                        row.addInt(colIdx, record.getAsInt(colName));
                        break;
                    case INT64:
                    case UNIXTIME_MICROS:
                        row.addLong(colIdx, record.getAsLong(colName));
                        break;
                    case STRING:
                        row.addString(colIdx, record.getAsString(colName));
                        break;
                    case DECIMAL32:
                    case DECIMAL64:
                    case DECIMAL128:
                        row.addDecimal(colIdx, new BigDecimal(record.getAsString(colName)));
                        break;
                    default:
                        throw new IllegalStateException(String.format("unknown column type %s", colType));
                }
            }
        }
    }

    private int getColumnIndex(Schema columns, String colName) {
        try {
            return columns.getColumnIndex(colName);
        } catch (Exception ex) {
            return -1;
        }
    }
}
