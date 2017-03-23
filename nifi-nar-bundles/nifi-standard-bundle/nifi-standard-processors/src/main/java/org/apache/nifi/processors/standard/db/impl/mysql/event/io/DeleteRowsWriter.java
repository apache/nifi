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
package org.apache.nifi.processors.standard.db.impl.mysql.event.io;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.standard.GetChangeDataCaptureMySQL;
import org.apache.nifi.processors.standard.db.event.ColumnDefinition;
import org.apache.nifi.processors.standard.db.impl.mysql.event.DeleteRowsEventInfo;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nifi.processors.standard.db.impl.mysql.MySQLCDCUtils.getWritableObject;

/**
 * A writer class to output MySQL binlog "delete rows" events to flow file(s).
 */
public class DeleteRowsWriter extends AbstractBinlogTableEventWriter<DeleteRowsEventInfo> {

    /**
     * Creates and transfers a new flow file whose contents are the JSON-serialized value of the specified event, and the sequence ID attribute set
     *
     * @param session   A reference to a ProcessSession from which the flow file(s) will be created and transferred
     * @param eventInfo An event whose value will become the contents of the flow file
     * @return The next available CDC sequence ID for use by the CDC processor
     */
    public long writeEvent(final ProcessSession session, final DeleteRowsEventInfo eventInfo, final long currentSequenceId) {
        final AtomicLong seqId = new AtomicLong(currentSequenceId);
        for (Serializable[] row : eventInfo.getRows()) {

            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, outputStream -> {

                super.startJson(outputStream, eventInfo);
                super.writeJson(eventInfo);

                final BitSet bitSet = eventInfo.getIncludedColumns();
                writeRow(eventInfo, row, bitSet);

                super.endJson();
            });

            flowFile = session.putAllAttributes(flowFile, getCommonAttributes(seqId.get(), eventInfo));
            session.transfer(flowFile, GetChangeDataCaptureMySQL.REL_SUCCESS);
            seqId.getAndIncrement();
        }
        return seqId.get();
    }

    protected void writeRow(DeleteRowsEventInfo event, Serializable[] row, BitSet includedColumns) throws IOException {
        jsonGenerator.writeArrayFieldStart("columns");
        int i = includedColumns.nextSetBit(0);
        while (i != -1) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeNumberField("id", i + 1);
            ColumnDefinition columnDefinition = event.getColumnByIndex(i);
            Integer columnType = null;
            if (columnDefinition != null) {
                jsonGenerator.writeStringField("name", columnDefinition.getName());
                columnType = (int) columnDefinition.getType();
                jsonGenerator.writeNumberField("column_type", columnType);
            }
            if (row[i] == null) {
                jsonGenerator.writeNullField("value");
            } else {
                jsonGenerator.writeObjectField("value", getWritableObject(columnType, row[i]));
            }
            jsonGenerator.writeEndObject();
            i = includedColumns.nextSetBit(i + 1);
        }
        jsonGenerator.writeEndArray();
    }
}
