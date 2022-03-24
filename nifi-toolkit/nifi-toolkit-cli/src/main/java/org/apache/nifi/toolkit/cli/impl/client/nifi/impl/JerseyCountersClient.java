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
package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import org.apache.nifi.toolkit.cli.impl.client.nifi.CountersClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.web.api.entity.CountersEntity;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;

public class JerseyCountersClient extends AbstractJerseyClient implements CountersClient {
    private final WebTarget countersTarget;

    public JerseyCountersClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyCountersClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.countersTarget = baseTarget.path("/counters");
    }

    @Override
    public CountersEntity getCounters() throws NiFiClientException, IOException {
        return executeAction("Error retrieving counters", () -> {
            return getRequestBuilder(countersTarget).get(CountersEntity.class);
        });

    }
}
