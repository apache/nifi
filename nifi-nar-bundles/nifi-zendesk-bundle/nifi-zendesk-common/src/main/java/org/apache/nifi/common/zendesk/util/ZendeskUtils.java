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
package org.apache.nifi.common.zendesk.util;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.common.zendesk.ZendeskProperties;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.web.client.api.HttpResponseEntity;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class ZendeskUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private ZendeskUtils() {
    }

    /**
     * Gets the body from the Http response.
     *
     * @param response Http response
     * @return response body
     */
    public static String getResponseBody(HttpResponseEntity response) {
        try (InputStream responseBodyStream = response.body()) {
            return new String(responseBodyStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Reading response body has failed", e);
        }
    }

    /**
     * Collects every non-blank dynamic property from the context.
     *
     * @param context    property context
     * @param properties property list from context
     * @param attributes attributes that Expressions can reference
     * @return Map of dynamic properties
     */
    public static Map<String, String> getDynamicProperties(PropertyContext context, Map<PropertyDescriptor, String> properties, Map<String, String> attributes) {
        return properties.entrySet().stream()
                // filter non-blank dynamic properties
                .filter(e -> e.getKey().isDynamic()
                        && StringUtils.isNotBlank(e.getValue())
                        && StringUtils.isNotBlank(context.getProperty(e.getKey()).evaluateAttributeExpressions(attributes).getValue())
                )
                // convert to Map keys and evaluated property values
                .collect(Collectors.toMap(
                        e -> e.getKey().getName(),
                        e -> context.getProperty(e.getKey()).evaluateAttributeExpressions(attributes).getValue()
                ));
    }

    /**
     * Creates the request object for the Zendesk ticket creation. The request objects root node will be created based on the number of tickets created.
     *
     * @param zendeskTickets list of tickets to be sent to the Zendesk API
     * @return input stream of the request object
     * @throws JsonProcessingException error while processing the request object
     */
    public static InputStream createRequestObject(List<ObjectNode> zendeskTickets) throws JsonProcessingException {
        ObjectNode ticketNode = OBJECT_MAPPER.createObjectNode();
        if (zendeskTickets.size() > 1) {
            ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
            for (ObjectNode ticket : zendeskTickets) {
                arrayNode.add(ticket);
            }

            ZendeskRecordPathUtils.addNewNodeAtPath(ticketNode, JsonPointer.compile(ZendeskProperties.ZENDESK_TICKETS_ROOT_NODE), arrayNode);
        } else {
            ZendeskRecordPathUtils.addNewNodeAtPath(ticketNode, JsonPointer.compile(ZendeskProperties.ZENDESK_TICKET_ROOT_NODE), zendeskTickets.get(0));
        }

        return new ByteArrayInputStream(OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(ticketNode).getBytes());
    }
}
