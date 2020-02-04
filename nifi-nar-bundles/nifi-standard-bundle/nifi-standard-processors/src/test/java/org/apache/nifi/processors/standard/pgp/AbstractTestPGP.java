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
package org.apache.nifi.processors.standard.pgp;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.openpgp.PGPException;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Map;

class AbstractTestPGP {
    static final String SERVICE_ID = AbstractProcessorPGP.SERVICE_ID;
    TestRunner runner;
    PGPKeyMaterialControllerService service;
    byte[] plainBytes;

    @BeforeClass
    public static void setupServiceControllerTestClass() throws IOException, PGPException {
        PGPKeyMaterialControllerServiceTest.setupKeyAndKeyRings();
    }

    @Before
    public void recreatePlainBytes() {
        plainBytes = Random.randomBytes(128 + Random.randomInt(128+1024));
    }

    public void recreateRunner(Processor processor) {
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(SERVICE_ID, SERVICE_ID);
    }

    public void recreateService(Map<String, String> properties) throws InitializationException {
        service = new PGPKeyMaterialControllerService();
        runner.addControllerService(SERVICE_ID, service, properties);
    }
}
