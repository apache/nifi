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
package org.apache.nifi.registry.client.impl;

import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.client.UserClient;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;

public class JerseyUserClient extends AbstractJerseyClient implements UserClient {

    private final WebTarget accessTarget;

    public JerseyUserClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyUserClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.accessTarget = baseTarget.path("/access");
    }

    @Override
    public CurrentUser getAccessStatus() throws NiFiRegistryException, IOException {
        return executeAction("Error retrieving access status for the current user", () -> {
            return getRequestBuilder(accessTarget).get(CurrentUser.class);
        });
    }
}
