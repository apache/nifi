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
import java.util.Map;
import javax.ws.rs.core.MediaType;
import org.apache.nifi.web.security.DnUtils;
import org.apache.nifi.web.security.x509.X509AuthenticationFilter;

/**
 *
 */
public class NiFiTestUser {

    public static final long REVISION = 0L;

    private final Client client;
    private final String proxyDn;

    public NiFiTestUser(Client client, String dn) {
        this.client = client;
        this.proxyDn = DnUtils.formatProxyDn(dn);
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
        // get the resource
        WebResource resource = client.resource(url);

        // append any query parameters
        if (queryParams != null && !queryParams.isEmpty()) {
            for (String key : queryParams.keySet()) {
                resource = resource.queryParam(key, queryParams.get(key));
            }
        }

        // perform the query
        return resource.accept(MediaType.APPLICATION_JSON).header(X509AuthenticationFilter.PROXY_ENTITIES_CHAIN, proxyDn).get(ClientResponse.class);
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
        // get the resource
        WebResource.Builder resourceBuilder = client.resource(url).accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).header(X509AuthenticationFilter.PROXY_ENTITIES_CHAIN, proxyDn);

        // include the request entity
        if (entity != null) {
            resourceBuilder = resourceBuilder.entity(entity);
        }

        // perform the request
        return resourceBuilder.post(ClientResponse.class);
    }

    /**
     * Performs a POST using the specified url and entity body.
     *
     * @param url url
     * @param entity entity
     * @return repsonse
     * @throws Exception ex
     */
    public ClientResponse testPostMultiPart(String url, Object entity) throws Exception {
        // get the resource
        WebResource.Builder resourceBuilder = client.resource(url).accept(MediaType.APPLICATION_XML).type(MediaType.MULTIPART_FORM_DATA).header(X509AuthenticationFilter.PROXY_ENTITIES_CHAIN, proxyDn);

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
     * @param url url
     * @param formData form data
     * @return response
     * @throws java.lang.Exception ex
     */
    public ClientResponse testPost(String url, Map<String, String> formData) throws Exception {
        // convert the form data
        MultivaluedMapImpl entity = new MultivaluedMapImpl();
        for (String key : formData.keySet()) {
            entity.add(key, formData.get(key));
        }

        // get the resource
        WebResource.Builder resourceBuilder
                = client.resource(url).accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_FORM_URLENCODED).header(X509AuthenticationFilter.PROXY_ENTITIES_CHAIN, proxyDn);

        // add the form data if necessary
        if (!entity.isEmpty()) {
            resourceBuilder = resourceBuilder.entity(entity);
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
        // get the resource
        WebResource.Builder resourceBuilder = client.resource(url).accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).header(X509AuthenticationFilter.PROXY_ENTITIES_CHAIN, proxyDn);

        // include the request entity
        if (entity != null) {
            resourceBuilder = resourceBuilder.entity(entity);
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
        // convert the form data
        MultivaluedMapImpl entity = new MultivaluedMapImpl();
        for (String key : formData.keySet()) {
            entity.add(key, formData.get(key));
        }

        // get the resource
        WebResource.Builder resourceBuilder
                = client.resource(url).accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_FORM_URLENCODED).header(X509AuthenticationFilter.PROXY_ENTITIES_CHAIN, proxyDn);

        // add the form data if necessary
        if (!entity.isEmpty()) {
            resourceBuilder = resourceBuilder.entity(entity);
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
        return testDelete(url, (Object) null);
    }

    /**
     * Performs a DELETE using the specified url and entity.
     *
     * @param url url
     * @param entity entity
     * @return repsonse
     * @throws java.lang.Exception ex
     */
    public ClientResponse testDelete(String url, Object entity) throws Exception {
        // get the resource
        WebResource.Builder resourceBuilder = client.resource(url).accept(MediaType.APPLICATION_JSON).header(X509AuthenticationFilter.PROXY_ENTITIES_CHAIN, proxyDn);

        // append any query parameters
        if (entity != null) {
            resourceBuilder = resourceBuilder.entity(entity);
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
        return resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_FORM_URLENCODED).header(X509AuthenticationFilter.PROXY_ENTITIES_CHAIN, proxyDn).delete(ClientResponse.class);
    }

}
