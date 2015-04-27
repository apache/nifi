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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import java.net.URI;
import java.util.Map;
import javax.ws.rs.core.MediaType;

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
     * @throws ClientHandlerException if issues occur handling the request
     * @throws UniformInterfaceException if any interface violations occur
     */
    public ClientResponse get(final URI uri) throws ClientHandlerException, UniformInterfaceException {
        return get(uri, null);
    }

    /**
     * Gets the content at the specified URI using the given query parameters.
     *
     * @param uri the URI to get the content of
     * @param queryParams the query parameters to use in the request
     * @return the client response resulting from getting the content of the URI
     * @throws ClientHandlerException if issues occur handling the request
     * @throws UniformInterfaceException if any interface violations occur
     */
    public ClientResponse get(final URI uri, final Map<String, String> queryParams) throws ClientHandlerException, UniformInterfaceException {
        // perform the request
        WebResource webResource = client.resource(uri);
        if (queryParams != null) {
            for (final Map.Entry<String, String> queryEntry : queryParams.entrySet()) {
                webResource = webResource.queryParam(queryEntry.getKey(), queryEntry.getValue());
            }
        }

        return webResource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    }

    /**
     * Performs a POST using the specified url and entity body.
     *
     * @param uri the URI to post to
     * @param entity the item to post
     * @return the client response of the request
     */
    public ClientResponse post(URI uri, Object entity) throws ClientHandlerException, UniformInterfaceException {
        // get the resource
        WebResource.Builder resourceBuilder = client.resource(uri).accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON);

        // include the request entity
        if (entity != null) {
            resourceBuilder = resourceBuilder.entity(entity);
        }

        // perform the request
        return resourceBuilder.post(ClientResponse.class);
    }

    /**
     * Performs a POST using the specified url and form data.
     *
     * @param uri the uri to post to
     * @param formData the data to post
     * @return the client reponse of the post
     */
    public ClientResponse post(URI uri, Map<String, String> formData) throws ClientHandlerException, UniformInterfaceException {
        // convert the form data
        MultivaluedMapImpl entity = new MultivaluedMapImpl();
        for (String key : formData.keySet()) {
            entity.add(key, formData.get(key));
        }

        // get the resource
        WebResource.Builder resourceBuilder = client.resource(uri).accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_FORM_URLENCODED);

        // add the form data if necessary
        if (!entity.isEmpty()) {
            resourceBuilder = resourceBuilder.entity(entity);
        }

        // perform the request
        return resourceBuilder.post(ClientResponse.class);
    }

    /**
     * Performs a HEAD request to the specified URI.
     *
     * @param uri the uri to request the head of
     * @return the client response of the request
     * @throws ClientHandlerException for issues handling the request
     * @throws UniformInterfaceException for issues with the request
     */
    public ClientResponse head(final URI uri) throws ClientHandlerException, UniformInterfaceException {
        // perform the request
        WebResource webResource = client.resource(uri);
        return webResource.head();
    }
}
