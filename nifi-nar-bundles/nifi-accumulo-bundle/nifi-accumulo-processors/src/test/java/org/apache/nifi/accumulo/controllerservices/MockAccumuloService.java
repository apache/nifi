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
package org.apache.nifi.accumulo.controllerservices;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;

import java.util.HashMap;
import java.util.Map;

public class MockAccumuloService {


    public static AccumuloService getService(final TestRunner runner, final String zk, final String instanceName, final String user, final String password) throws InitializationException {
        final AccumuloService accclient = new AccumuloService();
        Map<String,String> properties = new HashMap<>();
        properties.put(AccumuloService.ACCUMULO_PASSWORD.getName(), password);
        properties.put(AccumuloService.AUTHENTICATION_TYPE.getName(), "PASSWORD");
        properties.put(AccumuloService.ACCUMULO_USER.getName(), user);
        properties.put(AccumuloService.ZOOKEEPER_QUORUM.getName(), zk);
        properties.put(AccumuloService.INSTANCE_NAME.getName(), instanceName);
        runner.addControllerService("accclient", accclient, properties);
        runner.enableControllerService(accclient);
        runner.setProperty("accumulo-connector-service","accclient");
        return accclient;
    }
}
