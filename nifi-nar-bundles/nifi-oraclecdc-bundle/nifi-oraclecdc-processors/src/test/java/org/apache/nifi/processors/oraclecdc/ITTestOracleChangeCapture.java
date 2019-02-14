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
package org.apache.nifi.processors.oraclecdc;

import org.apache.nifi.processors.oraclecdc.controller.impl.StandardOracleCDCService;
import org.apache.nifi.processors.oraclecdc.controller.impl.StandardOracleClassLoaderService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class ITTestOracleChangeCapture {

    private TestRunner testRunner;

    @Before
    public void setup() throws InitializationException {
        testRunner = TestRunners.newTestRunner(OracleChangeCapture.class);
        // final DBCPService dbcp = new DBCPServiceSimpleImpl();
        // final Map<String, String> dbcpProperties = new HashMap<>();

        // testRunner.addControllerService("dbcp", dbcp, dbcpProperties);
        // testRunner.enableControllerService(dbcp);
        StandardOracleClassLoaderService clService = new StandardOracleClassLoaderService();
        StandardOracleCDCService service = new StandardOracleCDCService();
        testRunner.addControllerService("cdcservice", service);
        testRunner.addControllerService("clService", clService);
        testRunner.setProperty(service, StandardOracleCDCService.DB_HOST, "localhost");
        testRunner.setProperty(service, StandardOracleCDCService.DB_PORT, "32782");
        testRunner.setProperty(service, StandardOracleCDCService.DB_USER, "xstrmadmin");
        testRunner.setProperty(service, StandardOracleCDCService.DB_PASS, "welcome1");
        testRunner.setProperty(service, StandardOracleCDCService.DB_SID, "orcl");
        testRunner.setProperty(clService, StandardOracleClassLoaderService.DB_DRIVER_LOCATION,
                "/tmp/oracle/ojdbc8.jar," + "/tmp/oracle/xstreams.jar");
        testRunner.enableControllerService(clService);
        testRunner.setProperty(service, StandardOracleCDCService.DB_CLASS_LOADER, "clService");
        testRunner.enableControllerService(service);
        testRunner.setProperty(OracleChangeCapture.CDC_SERVICE, "cdcservice");

    }

    @Test
    public void testProcessor() {

        testRunner.setProperty(OracleChangeCapture.XS_OUT, "xout1");
        // testRunner.setRunSchedule(5000);
        testRunner.run(1);
        // testRunner.shutdown();
    }
}
