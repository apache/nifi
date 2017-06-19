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
import org.apache.kudu.Type;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Cam Mach - Inspur on 5/23/17.
 */

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"database", "NoSQL", "kudu", "inspur"})
@CapabilityDescription("Ingest data into Kudu table")
public class PutKudu extends AbstractKudu {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(KUDU_MASTERS);
        properties.add(TABLE_NAME);
        properties.add(RECORD_READER);
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
    protected OperationResponse createPut(ProcessSession session, ProcessContext context, FlowFile flowFile) {
        try {
            Set<Map.Entry<String, String>> attrSet = flowFile.getAttributes().entrySet();
            org.apache.kudu.client.Insert insert = kuduTable.newInsert();
            Schema colSchema = kuduTable.getSchema();
            PartialRow row = insert.getRow();

            for (Map.Entry<String, String> entry : attrSet) {
                String colName = entry.getKey();
                int colIdx = this.getColumnIndex(colSchema, colName);
                //getLogger().info("CreatePut: " + colName + " " + entry.getValue() + " - Found Col: " + colIdx);
                if (colIdx != -1) {
                    Type colType = colSchema.getColumn(colName).getType();
                    String val = entry.getValue();
                    switch (colType.getDataType()) {
                        case BOOL:
                            row.addBoolean(colIdx, Boolean.parseBoolean(val));
                            break;
                        case FLOAT:
                            row.addFloat(colIdx, Float.parseFloat(val));
                            break;
                        case DOUBLE:
                            row.addDouble(colIdx, Double.parseDouble(val));
                            break;
                        case BINARY:
                            row.addBinary(colIdx, val.getBytes());
                            break;
                        case INT8:
                        case INT16:
                            row.addShort(colIdx, Short.parseShort(val));
                            break;
                        case INT32:
                            row.addInt(colIdx, Integer.parseInt(val));
                            break;
                        case INT64:
                            row.addLong(colIdx, Long.parseLong(val));
                            break;
                        case STRING:
                            row.addString(colIdx, val);
                            break;
                        default:
                            throw new IllegalStateException(String.format("unknown column type %s", colType));
                    }
                }
            }

            return kuduSession.apply(insert);
        } catch (Exception ex) {
            getLogger().error("CamMach My Error: " + ex.getMessage() + "::" + ex.getStackTrace());
        }
        return null;
    }

    private int getColumnIndex(Schema columns, String colName) {
        try {
            return columns.getColumnIndex(colName);
        } catch (Exception ex) {
            return -1;
        }
    }
}
