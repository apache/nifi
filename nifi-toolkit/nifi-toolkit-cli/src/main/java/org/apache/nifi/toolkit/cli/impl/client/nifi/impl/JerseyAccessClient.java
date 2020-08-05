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

import org.apache.nifi.toolkit.cli.impl.client.nifi.AccessClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.util.StringUtils;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import java.io.IOException;

public class JerseyAccessClient extends AbstractJerseyClient implements AccessClient {

    private final WebTarget accessTarget;

    public JerseyAccessClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyAccessClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.accessTarget = baseTarget.path("/access");
    }

    @Override
    public String getToken(final String username, final String password) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(username)) {
            throw new IllegalArgumentException("Username is required");
        }

        if (StringUtils.isBlank(password)) {
            throw new IllegalArgumentException("Password is required");
        }

        return executeAction("Error performing login", () -> {
            final WebTarget target = accessTarget.path("token");

            final Form form = new Form();
            form.param("username", username);
            form.param("password", password);

            return getRequestBuilder(target).post(Entity.form(form), String.class);
        });
    }

    @Override
    public String getTokenFromSpnego() {
        // TODO implement
        return null;
    }
}
