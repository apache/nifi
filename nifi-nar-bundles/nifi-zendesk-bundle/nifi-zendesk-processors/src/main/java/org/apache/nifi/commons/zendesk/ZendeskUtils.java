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
package org.apache.nifi.commons.zendesk;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Base64.getEncoder;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.AUTHORIZATION_HEADER_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.BASIC_AUTH_PREFIX;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.CONTENT_TYPE_APPLICATION_JSON;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.CONTENT_TYPE_HEADER_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_TICKETS_ROOT_NODE;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_TICKET_ROOT_NODE;
import static org.apache.nifi.commons.zendesk.ZendeskRecordPathUtils.addNode;

public class ZendeskUtils {

    /**
     * Constructs the header for the Zendesk API call
     * @param authenticationContext authentication data holder object
     * @return the constructed header
     */
    public static String basicAuthHeaderValue(ZendeskAuthenticationContext authenticationContext) {
        final String user = authenticationContext.getAuthenticationType().enrichUserName(authenticationContext.getUser());
        final String userWithPassword = user + ":" + authenticationContext.getAuthenticationCredentials();
        return BASIC_AUTH_PREFIX + getEncoder().encodeToString(userWithPassword.getBytes());
    }

    public static String responseBodyToString(HttpResponseEntity response) {
        try {
            return IOUtils.toString(response.body(), UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Reading response body has failed", e);
        }
    }

    /**
     * Collects every non-blank dynamic property from the context.
     * @param context property context
     * @param properties property list
     * @return list of dynamic properties
     */
    public static Map<String, String> getDynamicProperties(PropertyContext context, Map<PropertyDescriptor, String> properties) {
        return properties.entrySet().stream()
                // filter non-blank dynamic properties
                .filter(e -> e.getKey().isDynamic()
                        && StringUtils.isNotBlank(e.getValue())
                        && StringUtils.isNotBlank(context.getProperty(e.getKey()).evaluateAttributeExpressions(context.getAllProperties()).getValue())
                )
                // convert to Map keys and evaluated property values
                .collect(Collectors.toMap(
                        e -> e.getKey().getName(),
                        e -> context.getProperty(e.getKey()).evaluateAttributeExpressions(context.getAllProperties()).getValue()
                ));
    }

    /**
     * Sends the ticket creation POST request to the Zendesk API.
     * @param authenticationContext authentication data holder object
     * @param webClientServiceProvider webclient provider
     * @param uri target uri
     * @param inputStream body of the request
     * @return response from the Zendesk API
     * @throws IOException error while performing the POST request
     */
    public static HttpResponseEntity performPostRequest(ZendeskAuthenticationContext authenticationContext,
                                                        WebClientServiceProvider webClientServiceProvider, URI uri, InputStream inputStream) throws IOException {
        return webClientServiceProvider.getWebClientService()
                .post()
                .uri(uri)
                .header(CONTENT_TYPE_HEADER_NAME, CONTENT_TYPE_APPLICATION_JSON)
                .header(AUTHORIZATION_HEADER_NAME, basicAuthHeaderValue(authenticationContext))
                .body(inputStream, OptionalLong.of(inputStream.available()))
                .retrieve();
    }

    /**
     * Creates the request object for the Zendesk ticket creation. The request objects root node will be created based on the number of tickets created.
     * @param mapper object mapper
     * @param zendeskTickets list of tickets to be sent to the Zendesk API
     * @return input stream of the request object
     * @throws JsonProcessingException error while processing the request object
     */
    public static InputStream createRequestObject(ObjectMapper mapper, List<ObjectNode> zendeskTickets) throws JsonProcessingException {
        ObjectNode ticketNode = mapper.createObjectNode();
        if (zendeskTickets.size() > 1) {
            ArrayNode arrayNode = mapper.createArrayNode();
            for (ObjectNode ticket : zendeskTickets) {
                arrayNode.add(ticket);
            }

            addNode(ticketNode, JsonPointer.compile(ZENDESK_TICKETS_ROOT_NODE), arrayNode);
        } else {
            addNode(ticketNode, JsonPointer.compile(ZENDESK_TICKET_ROOT_NODE), zendeskTickets.get(0));
        }

        return new ByteArrayInputStream(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(ticketNode).getBytes());
    }
}
