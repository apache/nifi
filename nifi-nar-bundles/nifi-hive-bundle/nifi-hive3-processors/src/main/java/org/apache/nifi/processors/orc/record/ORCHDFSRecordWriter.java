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
package org.apache.nifi.processors.orc.record;

import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.orc.PutORC.HIVE_DDL_ATTRIBUTE;

/**
 * HDFSRecordWriter that writes ORC files using the NiFi Record API as the schema representation.
 */

public class ORCHDFSRecordWriter implements HDFSRecordWriter {

    private final RecordSchema recordSchema;
    private final TypeInfo orcSchema;
    private final Writer orcWriter;
    private final String hiveTableName;
    private final boolean hiveFieldNames;
    private final List<RecordField> recordFields;
    private final int numRecordFields;
    private Object[] workingRow;

    public ORCHDFSRecordWriter(final Writer orcWriter, final RecordSchema recordSchema, final String hiveTableName, final boolean hiveFieldNames) {
        this.recordSchema = recordSchema;
        this.orcWriter = orcWriter;
        this.hiveFieldNames = hiveFieldNames;
        this.orcSchema = NiFiOrcUtils.getOrcSchema(recordSchema, this.hiveFieldNames);
        this.hiveTableName = hiveTableName;
        this.recordFields = recordSchema != null ? recordSchema.getFields() : null;
        this.numRecordFields = recordFields != null ? recordFields.size() : -1;
        // Reuse row object
        this.workingRow = numRecordFields > -1 ? new Object[numRecordFields] : null;
    }

    @Override
    public void write(final Record record) throws IOException {
        if (recordFields != null) {
            for (int i = 0; i < numRecordFields; i++) {
                final RecordField field = recordFields.get(i);
                final DataType fieldType = field.getDataType();
                final String fieldName = field.getFieldName();
                Object o = record.getValue(field);
                try {
                    workingRow[i] = NiFiOrcUtils.convertToORCObject(NiFiOrcUtils.getOrcField(fieldType, hiveFieldNames), o, hiveFieldNames);
                } catch (ArrayIndexOutOfBoundsException aioobe) {
                    final String errorMsg = "Index out of bounds for column " + i + ", type " + fieldName + ", and object " + o.toString();
                    throw new IOException(errorMsg, aioobe);
                }
            }

            orcWriter.addRow(NiFiOrcUtils.createOrcStruct(orcSchema, workingRow));
        }
    }

    /**
     * @param recordSet the RecordSet to write
     * @return the result of writing the record set
     * @throws IOException if an I/O error happens reading from the RecordSet, or writing a Record
     */
    public WriteResult write(final RecordSet recordSet) throws IOException {
        int recordCount = 0;

        Record record;
        while ((record = recordSet.next()) != null) {
            write(record);
            recordCount++;
        }

        // Add Hive DDL Attribute
        String hiveDDL = NiFiOrcUtils.generateHiveDDL(recordSchema, hiveTableName, hiveFieldNames);
        Map<String, String> attributes = new HashMap<String, String>() {{
            put(HIVE_DDL_ATTRIBUTE, hiveDDL);
        }};

        return WriteResult.of(recordCount, attributes);
    }

    @Override
    public void close() throws IOException {
        orcWriter.close();
    }

}

