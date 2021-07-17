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

import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public class NiFiTestUser {

    private final Client client;
    private final String proxyDn;
    private final Logger logger;

    public NiFiTestUser(Client client, String proxyDn) {
        logger = LoggerFactory.getLogger(NiFiTestUser.class);

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
    private Invocation.Builder addProxiedEntities(final Invocation.Builder builder) {
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
    public Response testGet(String url) throws Exception {
        return testGet(url, null);
    }

    /**
     * Performs a GET using the specified url and query parameters.
     *
     * @param url url
     * @param queryParams params
     * @return response
     */
    public Response testGet(String url, Map<String, String> queryParams) {
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
    public Response testGetWithHeaders(String url, Map<String, String> queryParams, Map<String, String> headers) {
        // get the resource
        WebTarget resource = client.target(url);

        // append any query parameters
        if (queryParams != null && !queryParams.isEmpty()) {
            for (String key : queryParams.keySet()) {
                resource = resource.queryParam(key, queryParams.get(key));
            }
        }

        // get the builder
        Invocation.Builder builder = addProxiedEntities(resource.request());

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                builder = builder.header(key, headers.get(key));
            }
        }

        // perform the query
        return builder.get();
    }

    /**
     * Performs a POST using the specified url.
     *
     * @param url url
     * @return response
     * @throws Exception ex
     */
    public Response testPost(String url) throws Exception {
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
    public Response testPost(String url, Object entity) throws Exception {
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
    public Response testPostWithHeaders(String url, Object entity, Map<String, String> headers) throws Exception {
        // get the resource
        Invocation.Builder resourceBuilder = addProxiedEntities(client.target(url).request());

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                resourceBuilder = resourceBuilder.header(key, headers.get(key));
            }
        }

        logger.info("POST Request to URL: " + url + " with headers: " + Arrays.toString(headers.entrySet().toArray()));

        // perform the request
        return resourceBuilder.post(Entity.json(entity));
    }

    /**
     * Performs a POST using the specified url and entity body.
     *
     * @param url url
     * @param entity entity
     * @return response
     * @throws Exception ex
     */
    public Response testPostMultiPart(String url, Object entity) throws Exception {
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
    public Response testPostMultiPartWithHeaders(String url, Object entity, Map<String, String> headers) throws Exception {
        // get the resource
        Invocation.Builder resourceBuilder = addProxiedEntities(client.target(url).request());

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                resourceBuilder = resourceBuilder.header(key, headers.get(key));
            }
        }

        // perform the request
        return resourceBuilder.post(Entity.entity(entity, MediaType.MULTIPART_FORM_DATA));
    }

    /**
     * Performs a POST using the specified url and form data.
     *
     * @param url url
     * @param formData form data
     * @return response
     * @throws java.lang.Exception ex
     */
    public Response testPost(String url, Map<String, String> formData) throws Exception {
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
    public Response testPostWithHeaders(String url, Map<String, String> formData, Map<String, String> headers) throws Exception {
        // convert the form data
        MultivaluedHashMap<String, String> entity = new MultivaluedHashMap();
        for (String key : formData.keySet()) {
            entity.add(key, formData.get(key));
        }

        // get the resource
        Invocation.Builder resourceBuilder = addProxiedEntities(client.target(url).request());

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                resourceBuilder = resourceBuilder.header(key, headers.get(key));
            }
        }

        // perform the request
        return resourceBuilder.post(Entity.form(entity));
    }

    /**
     * Performs a PUT using the specified url and entity body.
     *
     * @param url url
     * @param entity entity
     * @return response
     * @throws java.lang.Exception ex
     */
    public Response testPut(String url, Object entity) throws Exception {
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
    public Response testPutWithHeaders(String url, Object entity, Map<String, String> headers) throws Exception {
        // get the resource
        Invocation.Builder resourceBuilder = addProxiedEntities(client.target(url).request());

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                resourceBuilder = resourceBuilder.header(key, headers.get(key));
            }
        }

        // perform the request
        return resourceBuilder.put(Entity.json(entity));
    }

    /**
     * Performs a PUT using the specified url and form data.
     *
     * @param url url
     * @param formData form data
     * @return response
     * @throws java.lang.Exception ex
     */
    public Response testPut(String url, Map<String, String> formData) throws Exception {
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
    public Response testPutWithHeaders(String url, Map<String, String> formData, Map<String, String> headers) throws Exception {
        // convert the form data
        MultivaluedHashMap<String, String> entity = new MultivaluedHashMap();
        for (String key : formData.keySet()) {
            entity.add(key, formData.get(key));
        }

        // get the resource
        Invocation.Builder builder = addProxiedEntities(client.target(url).request());

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                builder = builder.header(key, headers.get(key));
            }
        }

        // perform the request
        return builder.put(Entity.form(entity));
    }

    /**
     * Performs a DELETE using the specified url.
     *
     * @param url url
     * @return response
     * @throws java.lang.Exception ex
     */
    public Response testDelete(String url) throws Exception {
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
    public Response testDeleteWithHeaders(String url, Map<String, String> headers) throws Exception {
        // get the resource
        Invocation.Builder builder = addProxiedEntities(client.target(url).request());

        // append any headers
        if (headers != null && !headers.isEmpty()) {
            for (String key : headers.keySet()) {
                builder = builder.header(key, headers.get(key));
            }
        }

        // perform the query
        return builder.delete();
    }

    /**
     * Performs a DELETE using the specified url and query parameters.
     *
     * @param url url
     * @param queryParams params
     * @return response
     * @throws java.lang.Exception ex
     */
    public Response testDelete(String url, Map<String, String> queryParams) throws Exception {
        // get the resource
        WebTarget target = client.target(url);

        // append any query parameters
        if (queryParams != null && !queryParams.isEmpty()) {
            for (String key : queryParams.keySet()) {
                target = target.queryParam(key, queryParams.get(key));
            }
        }

        // perform the request
        return addProxiedEntities(target.request()).delete();
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
    public Response testCreateToken(String url, String username, String password) throws Exception {
        // convert the form data
        MultivaluedHashMap<String, String> entity = new MultivaluedHashMap();
        entity.add("username", username);
        entity.add("password", password);

        // get the resource
        Invocation.Builder resourceBuilder = addProxiedEntities(client.target(url).request().accept(MediaType.TEXT_PLAIN));

        // perform the request
        return resourceBuilder.post(Entity.form(entity));
    }

}
