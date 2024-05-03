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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.util.StopWatch;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
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
    void processRecord(final List<FlowFile> flowFiles, final KinesisClientRecord kinesisRecord, final boolean lastRecord,
                       final ProcessSession session, final StopWatch stopWatch) {
        final String partitionKey = kinesisRecord.partitionKey();
        final String sequenceNumber = kinesisRecord.sequenceNumber();
        final Instant approximateArrivalTimestamp = kinesisRecord.approximateArrivalTimestamp();
        final ByteBuffer dataBuffer = kinesisRecord.data();
        byte[] data = dataBuffer != null ? new byte[dataBuffer.remaining()] : new byte[0];
        if (dataBuffer != null) {
            dataBuffer.get(data);
        }

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