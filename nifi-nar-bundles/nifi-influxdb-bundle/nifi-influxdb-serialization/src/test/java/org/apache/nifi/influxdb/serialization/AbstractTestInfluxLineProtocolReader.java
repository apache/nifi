/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.influxdb.serialization;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;


public abstract class AbstractTestInfluxLineProtocolReader {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected Map<String, String> variables;
    protected ComponentLog logger;

    protected InfluxLineProtocolReader readerFactory;
    protected TestRunner testRunner;

    @Before
    public void before() throws InitializationException {

        logger = Mockito.mock(ComponentLog.class);
        variables = Collections.emptyMap();

        readerFactory = new InfluxLineProtocolReader();

        testRunner = TestRunners.newTestRunner(new TestInfluxLineProtocolRecordReader.ReaderProcessor());
        testRunner.addControllerService("record-reader", readerFactory);
        testRunner.setProperty(readerFactory, InfluxLineProtocolReader.CHARSET, StandardCharsets.UTF_8.name());
        testRunner.enableControllerService(readerFactory);
    }
}
