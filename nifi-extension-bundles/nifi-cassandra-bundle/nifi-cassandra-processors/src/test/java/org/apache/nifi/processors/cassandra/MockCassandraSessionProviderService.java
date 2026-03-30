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
package org.apache.nifi.processors.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.cassandra.CassandraSessionProviderService;
import org.apache.nifi.controller.AbstractControllerService;

/**
 * Test-only Controller Service used to provide a mocked {@link CqlSession} to Cassandra processors.
 */
public class MockCassandraSessionProviderService extends AbstractControllerService implements CassandraSessionProviderService {
    private final CqlSession session;

    public MockCassandraSessionProviderService(final CqlSession session) {
        this.session = session;
    }

    @Override
    public CqlSession getCassandraSession() {
        return session;
    }
}
