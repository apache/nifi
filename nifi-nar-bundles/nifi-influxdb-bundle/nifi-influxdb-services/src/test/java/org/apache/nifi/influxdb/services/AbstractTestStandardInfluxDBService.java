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
package org.apache.nifi.influxdb.services;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public abstract class AbstractTestStandardInfluxDBService {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected StandardInfluxDBService service;
    protected TestRunner testRunner;

    protected void setUp(@NonNull final Supplier<Answer> answerConnect) throws Exception {

        service = Mockito.spy(new StandardInfluxDBService());

        // Mock response
        Mockito.doAnswer(invocation -> answerConnect.get().answer(invocation))
                .when(service)
                .connect(Mockito.anyString(),
                        Mockito.anyString(),
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.anyLong());

        testRunner = TestRunners.newTestRunner(ServiceProcessor.class);
        testRunner.addControllerService("influxdb-service", service);
    }

    protected void assertConnectToDatabase() throws IOException, GeneralSecurityException {

        InfluxDB influxDB = service.connect();

        QueryResult result = influxDB.query(new Query("SHOW USERS", null));
        List<String> usersColumns = result.getResults().get(0).getSeries().get(0).getColumns();

        Assert.assertTrue("Unexpected names of columns. Expected [user, admin] but current value is " + usersColumns,
                usersColumns.get(0).equals("user") && usersColumns.get(1).equals("admin"));
    }

    public static class ServiceProcessor extends AbstractProcessor {

        private static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
                .name("influxdb-service")
                .description("InfluxDBService")
                .identifiesControllerService(InfluxDBService.class)
                .required(true)
                .build();

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {

            List<PropertyDescriptor> descriptors = new ArrayList<>();
            descriptors.add(CLIENT_SERVICE);

            return descriptors;
        }
    }
}
