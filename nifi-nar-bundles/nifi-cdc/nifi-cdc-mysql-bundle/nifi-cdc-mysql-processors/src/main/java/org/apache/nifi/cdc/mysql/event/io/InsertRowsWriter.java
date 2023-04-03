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

import org.apache.nifi.cdc.event.io.EventWriterConfiguration;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.cdc.event.ColumnDefinition;
import org.apache.nifi.cdc.mysql.event.InsertRowsEventInfo;
import org.apache.nifi.processor.Relationship;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.BitSet;


/**
 * A writer class to output MySQL binlog "write rows" (aka INSERT) events to flow file(s).
 */
public class InsertRowsWriter extends AbstractBinlogTableEventWriter<InsertRowsEventInfo> {

    /**
     * Creates and transfers a new flow file whose contents are the JSON-serialized value of the specified event, and the sequence ID attribute set
     *
     * @param session   A reference to a ProcessSession from which the flow file(s) will be created and transferred
     * @param eventInfo An event whose value will become the contents of the flow file
     * @return The next available CDC sequence ID for use by the CDC processor
     */
    @Override
    public long writeEvent(final ProcessSession session, String transitUri, final InsertRowsEventInfo eventInfo, final long currentSequenceId, Relationship relationship,
                           final EventWriterConfiguration eventWriterConfiguration) {
        long seqId = currentSequenceId;
        for (Serializable[] row : eventInfo.getRows()) {
            configureEventWriter(eventWriterConfiguration, session, eventInfo);
            OutputStream outputStream = eventWriterConfiguration.getFlowFileOutputStream();
            try {

                super.startJson(outputStream, eventInfo);
                super.writeJson(eventInfo);

                final BitSet bitSet = eventInfo.getIncludedColumns();
                writeRow(eventInfo, row, bitSet);

                super.endJson();
            } catch (IOException ioe) {
                throw new UncheckedIOException("Write JSON start array failed", ioe);
            }

            eventWriterConfiguration.incrementNumberOfEventsWritten();

            // Check if it is time to finish the FlowFile
            if (maxEventsPerFlowFile(eventWriterConfiguration)
                    && eventWriterConfiguration.getNumberOfEventsWritten() == eventWriterConfiguration.getNumberOfEventsPerFlowFile()) {
                finishAndTransferFlowFile(session, eventWriterConfiguration, transitUri, seqId, eventInfo, relationship);
            }
            seqId++;
        }
        return seqId;
    }

    protected void writeRow(InsertRowsEventInfo event, Serializable[] row, BitSet includedColumns) throws IOException {
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
