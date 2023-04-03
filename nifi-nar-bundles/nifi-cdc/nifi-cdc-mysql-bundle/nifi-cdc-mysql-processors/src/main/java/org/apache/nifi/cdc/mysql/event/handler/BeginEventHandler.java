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

import com.github.shyiko.mysql.binlog.event.QueryEventData;
import org.apache.nifi.cdc.event.io.EventWriterConfiguration;
import org.apache.nifi.cdc.mysql.event.BeginTransactionEventInfo;
import org.apache.nifi.cdc.mysql.event.DataCaptureState;
import org.apache.nifi.cdc.mysql.event.io.AbstractBinlogEventWriter;
import org.apache.nifi.cdc.mysql.processors.CaptureChangeMySQL;
import org.apache.nifi.processor.ProcessSession;

import static org.apache.nifi.cdc.mysql.processors.CaptureChangeMySQL.REL_SUCCESS;

public class BeginEventHandler implements BinlogEventHandler<QueryEventData, BeginTransactionEventInfo> {
    @Override
    public void handleEvent(final QueryEventData eventData, final boolean writeEvent, DataCaptureState dataCaptureState,
                            CaptureChangeMySQL.BinlogResourceInfo binlogResourceInfo, CaptureChangeMySQL.BinlogEventState binlogEventState,
                            final String sql, AbstractBinlogEventWriter<BeginTransactionEventInfo> eventWriter,
                            EventWriterConfiguration eventWriterConfiguration, ProcessSession session, final long timestamp) {
        final String currentDatabase = eventData.getDatabase();
        BeginTransactionEventInfo beginEvent = dataCaptureState.isUseGtid()
                ? new BeginTransactionEventInfo(currentDatabase, timestamp, dataCaptureState.getGtidSet())
                : new BeginTransactionEventInfo(currentDatabase, timestamp, dataCaptureState.getBinlogFile(), dataCaptureState.getBinlogPosition());

        if (writeEvent) {
            binlogEventState.setCurrentEventInfo(beginEvent);
            binlogEventState.setCurrentEventWriter(eventWriter);
            dataCaptureState.setSequenceId(eventWriter.writeEvent(session, binlogResourceInfo.getTransitUri(), beginEvent, dataCaptureState.getSequenceId(),
                    REL_SUCCESS, eventWriterConfiguration));
        }
        binlogResourceInfo.setInTransaction(true);
    }
}
