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
package org.apache.nifi.marklogic.processor;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.impl.DatabaseClientImpl;
import com.marklogic.client.impl.OkHttpServices;

/* This class is used to mock MarkLogic interactions.
 * Stubbed out functions will be implemented as needed.
 * Extending the DatabaseClientImpl instead of implementing DatabaseClient
 * to avoid Checkstyle violation in pojoRepostory method declaration
 */
class TestMLDatabaseClient extends DatabaseClientImpl {
    static OkHttpServices services = new OkHttpServices();

    public TestMLDatabaseClient() {
        super(services, "", 0, "", null, ConnectionType.DIRECT);
    }

    @Override
    public DataMovementManager newDataMovementManager() {
        return new TestDataMovementManager();
    }
}
