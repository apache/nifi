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

import com.github.shyiko.mysql.binlog.event.EventData;
import org.apache.nifi.cdc.event.io.EventWriterConfiguration;
import org.apache.nifi.cdc.mysql.event.CommitTransactionEventInfo;
import org.apache.nifi.cdc.mysql.event.DataCaptureState;
import org.apache.nifi.cdc.mysql.event.io.CommitTransactionEventWriter;
import org.apache.nifi.cdc.mysql.processors.CaptureChangeMySQL;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import static org.apache.nifi.cdc.mysql.processors.CaptureChangeMySQL.REL_SUCCESS;

public class CommitEventHandler implements BinlogEventHandler<EventData, CommitTransactionEventInfo> {

    private final CommitTransactionEventWriter eventWriter = new CommitTransactionEventWriter();

    @Override
    public void handleEvent(final EventData eventData, final boolean writeEvent, final DataCaptureState dataCaptureState,
                            final CaptureChangeMySQL.BinlogResourceInfo binlogResourceInfo, final CaptureChangeMySQL.BinlogEventState binlogEventState,
                            final String sql, final EventWriterConfiguration eventWriterConfiguration, final ProcessSession session, final long timestamp) {
        final String currentDatabase = binlogResourceInfo.getCurrentDatabase();
        final CommitTransactionEventInfo commitEvent = dataCaptureState.isUseGtid()
                ? new CommitTransactionEventInfo(currentDatabase, timestamp, dataCaptureState.getGtidSet())
                : new CommitTransactionEventInfo(currentDatabase, timestamp, dataCaptureState.getBinlogFile(), dataCaptureState.getBinlogPosition());

        if (writeEvent) {
            binlogEventState.setCurrentEventInfo(commitEvent);
            binlogEventState.setCurrentEventWriter(eventWriter);
            dataCaptureState.setSequenceId(eventWriter.writeEvent(session, binlogResourceInfo.getTransitUri(), commitEvent, dataCaptureState.getSequenceId(),
                    REL_SUCCESS, eventWriterConfiguration));
        } else {
            // If the COMMIT event is not to be written, the FlowFile should still be finished and the session committed.
            if (session != null) {
                FlowFile flowFile = eventWriterConfiguration.getCurrentFlowFile();
                if (flowFile != null) {
                    // Flush the events to the FlowFile when the processor is stopped
                    eventWriter.finishAndTransferFlowFile(session, eventWriterConfiguration, binlogResourceInfo.getTransitUri(), dataCaptureState.getSequenceId(), commitEvent, REL_SUCCESS);
                }
                session.commitAsync();
            }
        }

        // Update inTransaction value to state
        binlogResourceInfo.setInTransaction(false);
    }
}