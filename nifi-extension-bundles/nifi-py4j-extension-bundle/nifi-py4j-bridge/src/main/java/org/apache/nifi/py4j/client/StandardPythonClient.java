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

package org.apache.nifi.py4j.client;

import org.apache.nifi.python.PythonController;
import py4j.Protocol;

public class StandardPythonClient {
    private static final String CONTROLLER_ID = Protocol.ENTRY_POINT_OBJECT_ID;
    private final PythonController pythonController;
    private final NiFiPythonGateway gateway;

    public StandardPythonClient(final NiFiPythonGateway gateway) {
        this.gateway = gateway;
        pythonController = new PythonProxy(gateway, CONTROLLER_ID,
            StandardPythonClient.class.getClassLoader(), new Class<?>[] {PythonController.class})
            .as(PythonController.class);
    }

    public NiFiPythonGateway getGateway() {
        return gateway;
    }

    public PythonController getController() {
        return pythonController;
    }
}
