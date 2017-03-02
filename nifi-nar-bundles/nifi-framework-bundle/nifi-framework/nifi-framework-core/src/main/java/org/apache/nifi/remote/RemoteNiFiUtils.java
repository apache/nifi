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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.nifi.web.util.WebUtils;

import javax.net.ssl.SSLContext;
import javax.ws.rs.core.MediaType;
import java.net.URI;

public class RemoteNiFiUtils {

    private static final int CONNECT_TIMEOUT = 10000;
    private static final int READ_TIMEOUT = 10000;

    private final Client client;

    public RemoteNiFiUtils(final SSLContext sslContext) {
        this.client = getClient(sslContext);
    }

    private Client getClient(final SSLContext sslContext) {
        final Client client;
        if (sslContext == null) {
            client = WebUtils.createClient(null);
        } else {
            client = WebUtils.createClient(null, sslContext);
        }

        client.setReadTimeout(READ_TIMEOUT);
        client.setConnectTimeout(CONNECT_TIMEOUT);

        return client;
    }

    /**
     * Issues a registration request for this NiFi instance for the instance at the baseApiUri.
     *
     * @param baseApiUri uri to register with
     * @return response
     */
    public ClientResponse issueRegistrationRequest(String baseApiUri) {
        final URI uri = URI.create(String.format("%s/controller/users", baseApiUri));

        // set up the query params
        MultivaluedMapImpl entity = new MultivaluedMapImpl();
        entity.add("justification", "A Remote instance of NiFi has attempted to create a reference to this NiFi. This action must be approved first.");

        // create the web resource
        WebResource webResource = client.resource(uri);

        // get the client utils and make the request
        return webResource.type(MediaType.APPLICATION_FORM_URLENCODED).entity(entity).post(ClientResponse.class);
    }
}
