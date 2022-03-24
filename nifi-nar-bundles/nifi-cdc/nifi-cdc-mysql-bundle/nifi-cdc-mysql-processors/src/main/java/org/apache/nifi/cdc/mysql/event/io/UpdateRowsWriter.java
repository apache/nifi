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
package org.apache.nifi.cdc.mysql.event.io;

import org.apache.nifi.cdc.mysql.event.MySQLCDCUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.cdc.event.ColumnDefinition;
import org.apache.nifi.cdc.mysql.event.UpdateRowsEventInfo;
import org.apache.nifi.processor.Relationship;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A writer class to output MySQL binlog "write rows" (aka INSERT) events to flow file(s).
 */
public class UpdateRowsWriter extends AbstractBinlogTableEventWriter<UpdateRowsEventInfo> {

    /**
     * Creates and transfers a new flow file whose contents are the JSON-serialized value of the specified event, and the sequence ID attribute set
     *
     * @param session   A reference to a ProcessSession from which the flow file(s) will be created and transferred
     * @param eventInfo An event whose value will become the contents of the flow file
     * @return The next available CDC sequence ID for use by the CDC processor
     */
    @Override
    public long writeEvent(final ProcessSession session, String transitUri, final UpdateRowsEventInfo eventInfo, final long currentSequenceId, Relationship relationship) {
        final AtomicLong seqId = new AtomicLong(currentSequenceId);
        for (Map.Entry<Serializable[], Serializable[]> row : eventInfo.getRows()) {

            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, outputStream -> {

                super.startJson(outputStream, eventInfo);
                super.writeJson(eventInfo);

                final BitSet bitSet = eventInfo.getIncludedColumns();
                writeRow(eventInfo, row, bitSet);

                super.endJson();
            });

            flowFile = session.putAllAttributes(flowFile, getCommonAttributes(seqId.get(), eventInfo));
            session.transfer(flowFile, relationship);
            session.getProvenanceReporter().receive(flowFile, transitUri);
            seqId.getAndIncrement();
        }
        return seqId.get();
    }

    protected void writeRow(UpdateRowsEventInfo event, Map.Entry<Serializable[], Serializable[]> row, BitSet includedColumns) throws IOException {

        jsonGenerator.writeArrayFieldStart("columns");
        int i = includedColumns.nextSetBit(0);
        while (i != -1) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeNumberField("id", i + 1);
            ColumnDefinition columnDefinition = event.getColumnByIndex(i);
            Integer columnType = null;
            if (columnDefinition != null) {
                jsonGenerator.writeStringField("name", columnDefinition.getName());
                columnType = columnDefinition.getType();
                jsonGenerator.writeNumberField("column_type", columnType);
            }
            Serializable[] oldRow = row.getKey();
            Serializable[] newRow = row.getValue();

            if (oldRow[i] == null) {
                jsonGenerator.writeNullField("last_value");
            } else {
                jsonGenerator.writeObjectField("last_value", MySQLCDCUtils.getWritableObject(columnType, oldRow[i]));
            }

            if (newRow[i] == null) {
                jsonGenerator.writeNullField("value");
            } else {
                jsonGenerator.writeObjectField("value", MySQLCDCUtils.getWritableObject(columnType, newRow[i]));
            }
            jsonGenerator.writeEndObject();
            i = includedColumns.nextSetBit(i + 1);
        }
        jsonGenerator.writeEndArray();
    }
}
