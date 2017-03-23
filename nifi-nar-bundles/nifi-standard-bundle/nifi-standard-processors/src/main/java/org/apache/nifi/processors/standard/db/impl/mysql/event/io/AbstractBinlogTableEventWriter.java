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
import org.apache.nifi.processors.standard.db.impl.mysql.event.BinlogTableEventInfo;

import java.io.IOException;

/**
 * An abstract base class for writing MYSQL binlog events into flow file(s), e.g.
 */
public abstract class AbstractBinlogTableEventWriter<T extends BinlogTableEventInfo> extends AbstractBinlogEventWriter<T> {

    protected void writeJson(T event) throws IOException {
        super.writeJson(event);
        if (event.getDatabaseName() != null) {
            jsonGenerator.writeStringField("database", event.getDatabaseName());
        } else {
            jsonGenerator.writeNullField("database");
        }
        if (event.getTableName() != null) {
            jsonGenerator.writeStringField("table_name", event.getTableName());
        } else {
            jsonGenerator.writeNullField("table_name");
        }
        if (event.getTableId() != null) {
            jsonGenerator.writeNumberField("table_id", event.getTableId());
        } else {
            jsonGenerator.writeNullField("table_id");
        }
    }

    // Default implementation for binlog events
    @Override
    public long writeEvent(ProcessSession session, T eventInfo, long currentSequenceId) {
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, (outputStream) -> {
            super.startJson(outputStream, eventInfo);
            super.writeJson(eventInfo);
            writeJson(eventInfo);
            // Nothing in the body
            super.endJson();
        });
        flowFile = session.putAllAttributes(flowFile, getCommonAttributes(currentSequenceId, eventInfo));
        session.transfer(flowFile, GetChangeDataCaptureMySQL.REL_SUCCESS);
        return currentSequenceId + 1;
    }
}
