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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;

import java.time.format.DateTimeFormatter;

public class KinesisRecordProcessorFactory implements IRecordProcessorFactory {
    private final ProcessSession session;
    private final ComponentLog log;
    private final String endpointPrefix;
    private final long checkpointIntervalMillis;
    private final long retryWaitMillis;
    private final int numRetries;
    private final DateTimeFormatter dateTimeFormatter;

    public KinesisRecordProcessorFactory(final ProcessSession session, final ComponentLog log, final String endpointPrefix,
                                  final long checkpointIntervalMillis, final long retryWaitMillis, final int numRetries,
                                  final DateTimeFormatter dateTimeFormatter) {
        this.session = session;
        this.log = log;
        this.endpointPrefix = endpointPrefix;
        this.checkpointIntervalMillis = checkpointIntervalMillis;
        this.retryWaitMillis = retryWaitMillis;
        this.numRetries = numRetries;
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisRecordProcessor(session, log, endpointPrefix, checkpointIntervalMillis, retryWaitMillis, numRetries, dateTimeFormatter);
    }
}