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
package org.apache.nifi.web.util;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Map;

/**
 *
 */
public class ClientUtils {

    private final Client client;

    public ClientUtils(Client client) {
        this.client = client;
    }

    /**
     * Gets the content at the specified URI.
     *
     * @param uri the URI to get the content of
     * @return the client response resulting from getting the content of the URI
     */
    public Response get(final URI uri) {
        return get(uri, null);
    }

    /**
     * Gets the content at the specified URI using the given query parameters.
     *
     * @param uri the URI to get the content of
     * @param queryParams the query parameters to use in the request
     * @return the client response resulting from getting the content of the URI
     */
    public Response get(final URI uri, final Map<String, String> queryParams) {
        // perform the request
        WebTarget webTarget = client.target(uri);
        if (queryParams != null) {
            for (final Map.Entry<String, String> queryEntry : queryParams.entrySet()) {
                webTarget = webTarget.queryParam(queryEntry.getKey(), queryEntry.getValue());
            }
        }

        return webTarget.request().accept(MediaType.APPLICATION_JSON).get();
    }

    /**
     * Performs a POST using the specified url and entity body.
     *
     * @param uri the URI to post to
     * @param entity the item to post
     * @return the client response of the request
     */
    public Response post(URI uri, Object entity) {
        // get the resource
        Invocation.Builder builder = client.target(uri).request().accept(MediaType.APPLICATION_JSON);

        // perform the request
        return builder.post(Entity.json(entity));
    }

    /**
     * Performs a POST using the specified url and form data.
     *
     * @param uri the uri to post to
     * @param formData the data to post
     * @return the client response of the post
     */
    public Response post(URI uri, Map<String, String> formData) {
        // convert the form data
        final MultivaluedHashMap<String, String> entity = new MultivaluedHashMap();
        for (String key : formData.keySet()) {
            entity.add(key, formData.get(key));
        }

        // get the resource
        Invocation.Builder builder = client.target(uri).request().accept(MediaType.APPLICATION_JSON);

        // get the resource
        return builder.post(Entity.form(entity));
    }

    /**
     * Performs a HEAD request to the specified URI.
     *
     * @param uri the uri to request the head of
     * @return the client response of the request
     */
    public Response head(final URI uri) {
        // perform the request
        WebTarget webTarget = client.target(uri);
        return webTarget.request().head();
    }
}
