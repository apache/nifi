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
package org.apache.nifi.remote;

import org.apache.nifi.web.util.WebUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import java.net.URI;

public class RemoteNiFiUtils {

    private static final int CONNECT_TIMEOUT = 10000;
    private static final int READ_TIMEOUT = 10000;

    private final Client client;

    public RemoteNiFiUtils(final SSLContext sslContext) {
        this.client = getClient(sslContext);
    }

    private Client getClient(final SSLContext sslContext) {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT);
        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);

        final Client client;
        if (sslContext == null) {
            client = WebUtils.createClient(clientConfig);
        } else {
            client = WebUtils.createClient(clientConfig, sslContext);
        }

        return client;
    }

    /**
     * Issues a registration request for this NiFi instance for the instance at the baseApiUri.
     *
     * @param baseApiUri uri to register with
     * @return response
     */
    public Response issueRegistrationRequest(String baseApiUri) {
        final URI uri = URI.create(String.format("%s/controller/users", baseApiUri));

        // set up the query params
        MultivaluedHashMap entity = new MultivaluedHashMap();
        entity.add("justification", "A Remote instance of NiFi has attempted to create a reference to this NiFi. This action must be approved first.");

        // get the resource
        return client.target(uri).request().post(Entity.form(entity));
    }
}
