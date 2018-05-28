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

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBIOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.CallsRealMethods;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.security.GeneralSecurityException;

public class TestStandardInfluxDBService extends AbstractTestStandardInfluxDBService {

    private Answer answerConnect;

    @Before
    public void before() throws Exception {

        setUp(() -> answerConnect);
    }

    @Test
    public void successConnect() throws IOException, GeneralSecurityException {

        answerConnect = new CallsRealMethods();

        testRunner.enableControllerService(service);

        InfluxDB influxDB = service.connect();

        Assert.assertNotNull(influxDB);
    }

    @Test
    public void errorHandling() throws IOException, GeneralSecurityException {

        answerConnect = invocation -> {

            throw new InfluxDBIOException(new IOException("simulate exception"));
        };

        testRunner.enableControllerService(service);

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Error while getting connection java.io.IOException: simulate exception");

        service.connect();
    }
}
