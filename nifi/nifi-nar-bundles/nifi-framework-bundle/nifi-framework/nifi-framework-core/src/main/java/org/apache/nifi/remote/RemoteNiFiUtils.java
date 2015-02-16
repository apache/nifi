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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.ws.rs.core.MediaType;

import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.util.WebUtils;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 *
 */
public class RemoteNiFiUtils {

    public static final String CONTROLLER_URI_PATH = "/controller";

    private static final int CONNECT_TIMEOUT = 10000;
    private static final int READ_TIMEOUT = 10000;
    
    private final Client client;
    
    public RemoteNiFiUtils(final SSLContext sslContext) {
        this.client = getClient(sslContext);
    }
    

    /**
     * Gets the content at the specified URI.
     *
     * @param uri
     * @param timeoutMillis
     * @return
     * @throws ClientHandlerException
     * @throws UniformInterfaceException
     */
    public ClientResponse get(final URI uri, final int timeoutMillis) throws ClientHandlerException, UniformInterfaceException {
        return get(uri, timeoutMillis, null);
    }
    
    /**
     * Gets the content at the specified URI using the given query parameters.
     *
     * @param uri
     * @param timeoutMillis
     * @param queryParams 
     * @return
     * @throws ClientHandlerException
     * @throws UniformInterfaceException
     */
    public ClientResponse get(final URI uri, final int timeoutMillis, final Map<String, String> queryParams) throws ClientHandlerException, UniformInterfaceException {
        // perform the request
        WebResource webResource = client.resource(uri);
        if ( queryParams != null ) {
            for ( final Map.Entry<String, String> queryEntry : queryParams.entrySet() ) {
                webResource = webResource.queryParam(queryEntry.getKey(), queryEntry.getValue());
            }
        }

        webResource.setProperty(ClientConfig.PROPERTY_READ_TIMEOUT, timeoutMillis);
        webResource.setProperty(ClientConfig.PROPERTY_CONNECT_TIMEOUT, timeoutMillis);

        return webResource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    }

    /**
     * Performs a HEAD request to the specified URI.
     *
     * @param uri
     * @param timeoutMillis
     * @return
     * @throws ClientHandlerException
     * @throws UniformInterfaceException
     */
    public ClientResponse head(final URI uri, final int timeoutMillis) throws ClientHandlerException, UniformInterfaceException {
        // perform the request
        WebResource webResource = client.resource(uri);
        webResource.setProperty(ClientConfig.PROPERTY_READ_TIMEOUT, timeoutMillis);
        webResource.setProperty(ClientConfig.PROPERTY_CONNECT_TIMEOUT, timeoutMillis);
        return webResource.head();
    }

    /**
     * Gets a client based on the specified URI.
     * 
     * @param uri
     * @return 
     */
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
     * Returns the port on which the remote instance is listening for Flow File transfers, or <code>null</code> if the remote instance
     * is not configured to use Site-to-Site transfers.
     * 
     * @param uri the base URI of the remote instance. This should include the path only to the nifi-api level, as well as the protocol, host, and port.
     * @param timeoutMillis
     * @return
     * @throws IOException
     */
    public Integer getRemoteListeningPort(final String uri, final int timeoutMillis) throws IOException {
    	try {
			final URI uriObject = new URI(uri + CONTROLLER_URI_PATH);
			return getRemoteListeningPort(uriObject, timeoutMillis);
		} catch (URISyntaxException e) {
			throw new IOException("Unable to establish connection to remote host because URI is invalid: " + uri);
		}
    }
    
    public String getRemoteRootGroupId(final String uri, final int timeoutMillis) throws IOException {
        try {
            final URI uriObject = new URI(uri + CONTROLLER_URI_PATH);
            return getRemoteRootGroupId(uriObject, timeoutMillis);
        } catch (URISyntaxException e) {
            throw new IOException("Unable to establish connection to remote host because URI is invalid: " + uri);
        }
    }
    
    public String getRemoteInstanceId(final String uri, final int timeoutMillis) throws IOException {
        try {
            final URI uriObject = new URI(uri + CONTROLLER_URI_PATH);
            return getController(uriObject, timeoutMillis).getInstanceId();
        } catch (URISyntaxException e) {
            throw new IOException("Unable to establish connection to remote host because URI is invalid: " + uri);
        }
    }
    
    /**
     * Returns the port on which the remote instance is listening for Flow File transfers, or <code>null</code> if the remote instance
     * is not configured to use Site-to-Site transfers.
     * 
     * @param uri the full URI to fetch, including the path.
     * @return
     * @throws IOException
     */
    private Integer getRemoteListeningPort(final URI uri, final int timeoutMillis) throws IOException {
    	return getController(uri, timeoutMillis).getRemoteSiteListeningPort();
    }
    
    private String getRemoteRootGroupId(final URI uri, final int timeoutMillis) throws IOException {
        return getController(uri, timeoutMillis).getId();
    }
    
    public ControllerDTO getController(final URI uri, final int timeoutMillis) throws IOException {
        final ClientResponse response = get(uri, timeoutMillis);
        
        if (Status.OK.getStatusCode() == response.getStatusInfo().getStatusCode()) {
            final ControllerEntity entity = response.getEntity(ControllerEntity.class);
            return entity.getController();
        } else {
            final String responseMessage = response.getEntity(String.class);
            throw new IOException("Got HTTP response Code " + response.getStatusInfo().getStatusCode() + ": " + response.getStatusInfo().getReasonPhrase() + " with explanation: " + responseMessage);
        }
    }
    
    /**
     * Issues a registration request on behalf of the current user.
     * 
     * @param baseApiUri 
     * @return  
     */
    public ClientResponse issueRegistrationRequest(String baseApiUri) {
        final URI uri = URI.create(String.format("%s/%s", baseApiUri, "/controller/users"));

        // set up the query params
        MultivaluedMapImpl entity = new MultivaluedMapImpl();
        entity.add("justification", "A Remote instance of NiFi has attempted to create a reference to this NiFi. This action must be approved first.");
        
        // create the web resource
        WebResource webResource = client.resource(uri);
        
        // get the client utils and make the request
        return webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_FORM_URLENCODED).entity(entity).post(ClientResponse.class);
    }
}
