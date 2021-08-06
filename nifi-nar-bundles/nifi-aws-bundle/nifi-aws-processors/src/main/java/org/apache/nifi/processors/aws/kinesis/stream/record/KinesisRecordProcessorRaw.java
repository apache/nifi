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
package org.apache.nifi.processors.aws.kinesis.stream.record;

import com.amazonaws.services.kinesis.model.Record;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.util.StopWatch;

import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class KinesisRecordProcessorRaw extends AbstractKinesisRecordProcessor {
    public KinesisRecordProcessorRaw(final ProcessSessionFactory sessionFactory, final ComponentLog log, final String streamName,
                                     final String endpointPrefix, final String kinesisEndpoint,
                                     final long checkpointIntervalMillis, final long retryWaitMillis,
                                     final int numRetries, final DateTimeFormatter dateTimeFormatter) {
        super(sessionFactory, log, streamName, endpointPrefix, kinesisEndpoint, checkpointIntervalMillis, retryWaitMillis,
                numRetries, dateTimeFormatter);
    }

    @Override
    void processRecord(final List<FlowFile> flowFiles, final Record kinesisRecord, final boolean lastRecord,
                       final ProcessSession session, final StopWatch stopWatch) {
        final String partitionKey = kinesisRecord.getPartitionKey();
        final String sequenceNumber = kinesisRecord.getSequenceNumber();
        final Date approximateArrivalTimestamp = kinesisRecord.getApproximateArrivalTimestamp();
        final byte[] data = kinesisRecord.getData() != null ? kinesisRecord.getData().array() : new byte[0];

        FlowFile flowFile = session.create();
        session.write(flowFile, out -> out.write(data));

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Sequence No: {}, Partition Key: {}, Data: {}", sequenceNumber, partitionKey, BASE_64_ENCODER.encodeToString(data));
        }

        reportProvenance(session, flowFile, partitionKey, sequenceNumber, stopWatch);

        final Map<String, String> attributes = getDefaultAttributes(sequenceNumber, partitionKey, approximateArrivalTimestamp);
        flowFile = session.putAllAttributes(flowFile, attributes);

        flowFiles.add(flowFile);
    }
}