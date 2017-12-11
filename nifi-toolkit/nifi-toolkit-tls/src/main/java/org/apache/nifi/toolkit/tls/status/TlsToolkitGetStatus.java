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
package org.apache.nifi.toolkit.tls.status;

import org.apache.nifi.toolkit.tls.configuration.GetStatusConfig;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

public class TlsToolkitGetStatus {

    public void get(final GetStatusConfig config) {
        final SSLContext sslContext = config.getSslContext();

        final ClientBuilder clientBuilder = ClientBuilder.newBuilder();
        if (sslContext != null) {
            clientBuilder.sslContext(sslContext);
        }

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 10000);
        clientConfig.property(ClientProperties.READ_TIMEOUT, 10000);
        clientBuilder.withConfig(clientConfig);

        final Client client = clientBuilder.build();
        final WebTarget target = client.target(config.getUrl());
        final Response response = target.request().get();
        System.out.println("Response Code - " + response.getStatus());
    }

}
