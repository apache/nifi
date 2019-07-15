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
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.aws.kinesis.stream.GetKinesisStream;
import org.apache.nifi.processors.aws.kinesis.stream.record.KinesisRecordProcessor;
import org.apache.nifi.processors.aws.kinesis.stream.record.KinesisRecordProcessorFactory;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestKinesisRecordProcessorFactory {
    @Test
    public void testKinesisRecordProcessorFactory() {
        final TestRunner runner = TestRunners.newTestRunner(GetKinesisStream.class);
        final ProcessSession session = new MockProcessSession(new SharedSessionState(runner.getProcessor(), new AtomicLong(0)), runner.getProcessor());
        final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

        final IRecordProcessorFactory fixture = new KinesisRecordProcessorFactory(session, runner.getLogger(), "kinesis-test", 10, 20, 5, dtf);
        final IRecordProcessor recordProcessor = fixture.createProcessor();
        assertThat(recordProcessor, instanceOf(KinesisRecordProcessor.class));

        final KinesisRecordProcessor result = (KinesisRecordProcessor) recordProcessor;
        assertThat(result.session, sameInstance(session));
        assertThat(result.log, sameInstance(runner.getLogger()));
        assertThat(result.endpointPrefix, equalTo("kinesis-test"));
        assertThat(result.checkpointIntervalMillis, equalTo(10L));
        assertThat(result.retryWaitMillis, equalTo(20L));
        assertThat(result.numRetries, equalTo(5));
        assertThat(result.dateTimeFormatter, sameInstance(dtf));
    }
}
