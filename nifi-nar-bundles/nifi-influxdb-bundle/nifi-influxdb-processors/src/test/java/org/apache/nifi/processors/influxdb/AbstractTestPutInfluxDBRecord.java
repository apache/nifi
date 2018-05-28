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
package org.apache.nifi.processors.influxdb;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.influxdb.services.InfluxDBService;
import org.apache.nifi.influxdb.services.StandardInfluxDBService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.hamcrest.Description;
import org.hamcrest.core.IsInstanceOf;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.security.GeneralSecurityException;

public abstract class AbstractTestPutInfluxDBRecord {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected InfluxDB influxDB = Mockito.mock(InfluxDB.class);
    protected Answer writeAnswer = invocation -> Void.class;
    protected ArgumentCaptor<BatchPoints> pointCapture = ArgumentCaptor.forClass(BatchPoints.class);

    protected TestRunner testRunner;
    protected MockRecordParser recordReader;
    protected MockComponentLog logger;

    protected PutInfluxDBRecord processor;

    @Before
    public void before() throws InitializationException, IOException, GeneralSecurityException {

        processor = Mockito.spy(new PutInfluxDBRecord());

        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setProperty(PutInfluxDBRecord.DB_NAME, "nifi-database");
        testRunner.setProperty(PutInfluxDBRecord.MEASUREMENT, "nifi-measurement");
        testRunner.setProperty(PutInfluxDBRecord.FIELDS, "nifi-field");
        testRunner.setProperty(PutInfluxDBRecord.RECORD_READER_FACTORY, "recordReader");
        testRunner.setProperty(PutInfluxDBRecord.INFLUX_DB_SERVICE, "influxdb-service");

        recordReader = new MockRecordParser();
        testRunner.addControllerService("recordReader", recordReader);
        testRunner.enableControllerService(recordReader);

        InfluxDBService influxDBService = Mockito.spy(new StandardInfluxDBService());
        Mockito.doAnswer(invocation -> {

            Mockito.doAnswer(writeAnswer).when(influxDB).write(pointCapture.capture());

            return influxDB;

        }).when(influxDBService).connect();

        testRunner.addControllerService("influxdb-service", influxDBService);
        testRunner.enableControllerService(influxDBService);

        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);
        logger = initContext.getLogger();
        processor.initialize(initContext);

        /**
         * Manually call onScheduled because it si not called by
         * org.apache.nifi.util.StandardProcessorTestRunner#run(int, boolean, boolean, long).
         * Bug in Mockito-CGLIB (https://github.com/mockito/mockito/issues/204)... remove after upgrade Mockito to 2.x
         */
        processor.onScheduled(testRunner.getProcessContext());
    }

    @After
    public void after() {

        processor.close();
    }

    protected class TypeOfExceptionMatcher<T extends Throwable> extends IsInstanceOf {

        private final Class<T> expectedErrorType;

        public TypeOfExceptionMatcher(final Class<T> expectedClass) {

            super(expectedClass);

            this.expectedErrorType = expectedClass;
        }

        @Override
        protected boolean matches(final Object item, final Description description) {

            return ExceptionUtils.indexOfType((Throwable) item, expectedErrorType) != -1;
        }
    }
}
