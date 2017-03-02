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
package org.apache.nifi.integration.util;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;

import javax.ws.rs.core.MediaType;
import java.util.Map;

/**
 *
 */
public class NiFiTestUser {

    private final Client client;
    private final String proxyDn;

    public NiFiTestUser(Client client, String proxyDn) {
        this.client = client;
        if (proxyDn != null) {
            this.proxyDn = ProxiedEntitiesUtils.formatProxyDn(proxyDn);
        } else {
            this.proxyDn = null;
        }
    }

    /**
     * Conditionally adds the proxied entities chain.
     *
     * @param builder the resource builder
     * @return the resource builder
     */
    private WebResource.Builder addProxiedEntities(final WebResource.Builder builder) {
        if (proxyDn == null) {
            return builder;
        } else {
            return builder.header(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, proxyDn);
        }
    }

    /**
     * Performs a GET using the specified url.
     *
     * @param url url
     * @return response
     * @throws Exception ex
     */
    public ClientResponse testGet(String url) throws Exception {
        return testGet(url, null);
    }

    /**
     * Performs a GET using the specified url and query parameters.
     *
     * @param url url
     * @param queryParams params
     * @return response
     */
    public ClientResponse testGet(String url, Map<String, String> queryParams) {
        return testGetWithHeaders(url, queryParams, null);
    }

    /**
     * Performs a GET using the specified url and query parameters.
     *
     * @param url url
     * @param queryParams params
     * @param headers http headers
     * @return response
     */
    public ClientResponse testGetWithHeaders(String url, Map<String, String> queryParams, Map<String, String> headers) {
        // get the resource
        WebResource resource = client.resource(url);

        // append any query parameters
        if (queryParams != null && !queryParams.isEmpty()) {
            for (String key : queryParams.keySet()) {
                resource = resource.queryParam(key, queryParams.get(key));
            }
        }

        // get the builder
        WebResource.Builder builder = addProxiedEntities(resource.getRequestBuilder());

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                builder = builder.header(key, headers.get(key));
            }
        }

        // perform the query
        return builder.get(ClientResponse.class);
    }

    /**
     * Performs a POST using the specified url.
     *
     * @param url url
     * @return response
     * @throws Exception ex
     */
    public ClientResponse testPost(String url) throws Exception {
        return testPost(url, (Object) null);
    }

    /**
     * Performs a POST using the specified url and entity body.
     *
     * @param url url
     * @param entity entity
     * @return response
     * @throws Exception ex
     */
    public ClientResponse testPost(String url, Object entity) throws Exception {
        return testPostWithHeaders(url, entity, null);
    }

    /**
     * Performs a POST using the specified url and entity body.
     *
     * @param url url
     * @param entity entity
     * @param headers http headers
     * @return response
     * @throws Exception ex
     */
    public ClientResponse testPostWithHeaders(String url, Object entity, Map<String, String> headers) throws Exception {
        // get the resource
        WebResource.Builder resourceBuilder = addProxiedEntities(client.resource(url).type(MediaType.APPLICATION_JSON));

        // include the request entity
        if (entity != null) {
            resourceBuilder = resourceBuilder.entity(entity);
        }

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                resourceBuilder = resourceBuilder.header(key, headers.get(key));
            }
        }

        // perform the request
        return resourceBuilder.post(ClientResponse.class);
    }

    /**
     * Performs a POST using the specified url and entity body.
     *
     * @param url url
     * @param entity entity
     * @return response
     * @throws Exception ex
     */
    public ClientResponse testPostMultiPart(String url, Object entity) throws Exception {
        return testPostMultiPartWithHeaders(url, entity, null);
    }

    /**
     * Performs a POST using the specified url and entity body.
     *
     * @param url url
     * @param entity entity
     * @param headers http headers
     * @return response
     * @throws Exception ex
     */
    public ClientResponse testPostMultiPartWithHeaders(String url, Object entity, Map<String, String> headers) throws Exception {
        // get the resource
        WebResource.Builder resourceBuilder = addProxiedEntities(client.resource(url).accept(MediaType.APPLICATION_XML).type(MediaType.MULTIPART_FORM_DATA));

        // include the request entity
        if (entity != null) {
            resourceBuilder = resourceBuilder.entity(entity);
        }

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                resourceBuilder = resourceBuilder.header(key, headers.get(key));
            }
        }

        // perform the request
        return resourceBuilder.post(ClientResponse.class);
    }

    /**
     * Performs a POST using the specified url and form data.
     *
     * @param url url
     * @param formData form data
     * @return response
     * @throws java.lang.Exception ex
     */
    public ClientResponse testPost(String url, Map<String, String> formData) throws Exception {
        return testPostWithHeaders(url, formData, null);
    }

    /**
     * Performs a POST using the specified url and form data.
     *
     * @param url url
     * @param formData form data
     * @param headers http headers
     * @return response
     * @throws java.lang.Exception ex
     */
    public ClientResponse testPostWithHeaders(String url, Map<String, String> formData, Map<String, String> headers) throws Exception {
        // convert the form data
        MultivaluedMapImpl entity = new MultivaluedMapImpl();
        for (String key : formData.keySet()) {
            entity.add(key, formData.get(key));
        }

        // get the resource
        WebResource.Builder resourceBuilder = addProxiedEntities(client.resource(url).type(MediaType.APPLICATION_FORM_URLENCODED));

        // add the form data if necessary
        if (!entity.isEmpty()) {
            resourceBuilder = resourceBuilder.entity(entity);
        }

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                resourceBuilder = resourceBuilder.header(key, headers.get(key));
            }
        }

        // perform the request
        return resourceBuilder.post(ClientResponse.class);
    }

    /**
     * Performs a PUT using the specified url and entity body.
     *
     * @param url url
     * @param entity entity
     * @return response
     * @throws java.lang.Exception ex
     */
    public ClientResponse testPut(String url, Object entity) throws Exception {
        return testPutWithHeaders(url, entity, null);
    }

    /**
     * Performs a PUT using the specified url and entity body.
     *
     * @param url url
     * @param entity entity
     * @param headers http headers
     * @return response
     * @throws java.lang.Exception ex
     */
    public ClientResponse testPutWithHeaders(String url, Object entity, Map<String, String> headers) throws Exception {
        // get the resource
        WebResource.Builder resourceBuilder = addProxiedEntities(client.resource(url).type(MediaType.APPLICATION_JSON));

        // include the request entity
        if (entity != null) {
            resourceBuilder = resourceBuilder.entity(entity);
        }

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                resourceBuilder = resourceBuilder.header(key, headers.get(key));
            }
        }

        // perform the request
        return resourceBuilder.put(ClientResponse.class);
    }

    /**
     * Performs a PUT using the specified url and form data.
     *
     * @param url url
     * @param formData form data
     * @return response
     * @throws java.lang.Exception ex
     */
    public ClientResponse testPut(String url, Map<String, String> formData) throws Exception {
        return testPutWithHeaders(url, formData, null);
    }

    /**
     * Performs a PUT using the specified url and form data.
     *
     * @param url url
     * @param formData form data
     * @param headers http headers
     * @return response
     * @throws java.lang.Exception ex
     */
    public ClientResponse testPutWithHeaders(String url, Map<String, String> formData, Map<String, String> headers) throws Exception {
        // convert the form data
        MultivaluedMapImpl entity = new MultivaluedMapImpl();
        for (String key : formData.keySet()) {
            entity.add(key, formData.get(key));
        }

        // get the resource
        WebResource.Builder resourceBuilder = addProxiedEntities(client.resource(url).type(MediaType.APPLICATION_FORM_URLENCODED));

        // add the form data if necessary
        if (!entity.isEmpty()) {
            resourceBuilder = resourceBuilder.entity(entity);
        }

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                resourceBuilder = resourceBuilder.header(key, headers.get(key));
            }
        }

        // perform the request
        return resourceBuilder.put(ClientResponse.class);
    }

    /**
     * Performs a DELETE using the specified url.
     *
     * @param url url
     * @return response
     * @throws java.lang.Exception ex
     */
    public ClientResponse testDelete(String url) throws Exception {
        return testDelete(url, null);
    }

    /**
     * Performs a DELETE using the specified url and entity.
     *
     * @param url url
     * @param headers http headers
     * @return response
     * @throws java.lang.Exception ex
     */
    public ClientResponse testDeleteWithHeaders(String url, Map<String, String> headers) throws Exception {
        // get the resource
        WebResource.Builder resourceBuilder = addProxiedEntities(client.resource(url).getRequestBuilder());

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                resourceBuilder = resourceBuilder.header(key, headers.get(key));
            }
        }

        // perform the query
        return resourceBuilder.delete(ClientResponse.class);
    }

    /**
     * Performs a DELETE using the specified url and query parameters.
     *
     * @param url url
     * @param queryParams params
     * @return response
     * @throws java.lang.Exception ex
     */
    public ClientResponse testDelete(String url, Map<String, String> queryParams) throws Exception {
        // get the resource
        WebResource resource = client.resource(url);

        // append any query parameters
        if (queryParams != null && !queryParams.isEmpty()) {
            for (String key : queryParams.keySet()) {
                resource = resource.queryParam(key, queryParams.get(key));
            }
        }

        // perform the request
        return addProxiedEntities(resource.type(MediaType.APPLICATION_FORM_URLENCODED)).delete(ClientResponse.class);
    }

    /**
     * Attempts to create a token with the specified username and password.
     *
     * @param url the url
     * @param username the username
     * @param password the password
     * @return response
     * @throws Exception ex
     */
    public ClientResponse testCreateToken(String url, String username, String password) throws Exception {
        // convert the form data
        MultivaluedMapImpl entity = new MultivaluedMapImpl();
        entity.add("username", username);
        entity.add("password", password);

        // get the resource
        WebResource.Builder resourceBuilder = addProxiedEntities(client.resource(url).accept(MediaType.TEXT_PLAIN).type(MediaType.APPLICATION_FORM_URLENCODED)).entity(entity);

        // perform the request
        return resourceBuilder.post(ClientResponse.class);
    }

}
