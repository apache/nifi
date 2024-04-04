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
package org.apache.nifi.cdc.mysql.event.handler;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import org.apache.nifi.cdc.event.io.EventWriterConfiguration;
import org.apache.nifi.cdc.mysql.event.DataCaptureState;
import org.apache.nifi.cdc.mysql.event.DeleteRowsEventInfo;
import org.apache.nifi.cdc.mysql.event.io.DeleteRowsWriter;
import org.apache.nifi.cdc.mysql.processors.CaptureChangeMySQL;
import org.apache.nifi.processor.ProcessSession;

import static org.apache.nifi.cdc.mysql.processors.CaptureChangeMySQL.REL_SUCCESS;

public class DeleteEventHandler implements BinlogEventHandler<DeleteRowsEventData, DeleteRowsEventInfo> {

    private final DeleteRowsWriter eventWriter = new DeleteRowsWriter();

    @Override
    public void handleEvent(final DeleteRowsEventData eventData, final boolean writeEvent, final DataCaptureState dataCaptureState,
                            final CaptureChangeMySQL.BinlogResourceInfo binlogResourceInfo, final CaptureChangeMySQL.BinlogEventState binlogEventState,
                            final String sql, final EventWriterConfiguration eventWriterConfiguration, final ProcessSession session, final long timestamp) {
        if (writeEvent) {
            final DeleteRowsEventInfo eventInfo = dataCaptureState.isUseGtid()
                    ? new DeleteRowsEventInfo(binlogResourceInfo.getCurrentTable(), timestamp, dataCaptureState.getGtidSet(), eventData)
                    : new DeleteRowsEventInfo(binlogResourceInfo.getCurrentTable(), timestamp, dataCaptureState.getBinlogFile(), dataCaptureState.getBinlogPosition(), eventData);

            binlogEventState.setCurrentEventInfo(eventInfo);
            binlogEventState.setCurrentEventWriter(eventWriter);
            dataCaptureState.setSequenceId(eventWriter.writeEvent(session, binlogResourceInfo.getTransitUri(), eventInfo, dataCaptureState.getSequenceId(),
                    REL_SUCCESS, eventWriterConfiguration));
        }
    }
}
