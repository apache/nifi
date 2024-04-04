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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.record.path.property.RecordPathPropertyUtil;
import org.apache.nifi.serialization.record.Record;

import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_TICKETS_ROOT_NODE;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_TICKET_ROOT_NODE;

public final class ZendeskRecordPathUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private ZendeskRecordPathUtils() {
    }

    /**
     * Resolves the property value and appends it as a new node at the given path.
     *
     * @param path           path in the request object
     * @param value          property value to be resolved
     * @param baseTicketNode base request object where the field value will be added
     * @param record         record to receive the value from if the field value is a record path
     */
    public static void addField(String path, String value, ObjectNode baseTicketNode, Record record) {
        final String resolvedValue = RecordPathPropertyUtil.resolvePropertyValue(value, record);

        if (resolvedValue != null) {
            addNewNodeAtPath(baseTicketNode, JsonPointer.compile(path), new TextNode(resolvedValue));
        }
    }

    /**
     * Adds a user defined dynamic field to the request object. If the user specifies the path in the request object as full path (starting with '/ticket' or '/tickets')
     * the method removes them since the root path is specified later based on the number of records.
     *
     * @param path           path in the request object
     * @param value          dynamic field value
     * @param baseTicketNode base request object where the field value will be added
     * @param record         record to receive the value from if the field value is a record path
     */
    public static void addDynamicField(String path, String value, ObjectNode baseTicketNode, Record record) {
        if (path.startsWith(ZENDESK_TICKET_ROOT_NODE)) {
            path = path.substring(7);
        } else if (path.startsWith(ZENDESK_TICKETS_ROOT_NODE)) {
            path = path.substring(8);
        }

        addField(path, value, baseTicketNode, record);
    }

    /**
     * Adds a new node on the provided path with the give value to the request object.
     *
     * @param baseNode base object where the new node will be added
     * @param path     path of the new node
     * @param value    value of the new node
     */
    public static void addNewNodeAtPath(final ObjectNode baseNode, final JsonPointer path, final JsonNode value) {
        final JsonPointer parentPointer = path.head();
        final String fieldName = path.last().toString().substring(1);
        JsonNode parentNode = getOrCreateParentNode(baseNode, parentPointer, fieldName);

        setNodeValue(value, fieldName, parentNode);
    }

    private static void setNodeValue(JsonNode value, String fieldName, JsonNode parentNode) {
        if (parentNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) parentNode;
            int index = Integer.parseInt(fieldName);
            for (int i = arrayNode.size(); i <= index; i++) {
                arrayNode.addNull();
            }
            arrayNode.set(index, value);
        } else if (parentNode.isObject()) {
            ((ObjectNode) parentNode).set(fieldName, value);
        } else {
            throw new IllegalArgumentException("Unsupported node type" + parentNode.getNodeType().name());
        }
    }

    private static JsonNode getOrCreateParentNode(ObjectNode rootNode, JsonPointer parentPointer, String fieldName) {
        JsonNode parentNode = rootNode.at(parentPointer);

        if (parentNode.isMissingNode() || parentNode.isNull()) {
            parentNode = StringUtils.isNumeric(fieldName) ? OBJECT_MAPPER.createArrayNode() : OBJECT_MAPPER.createObjectNode();
            addNewNodeAtPath(rootNode, parentPointer, parentNode);
        }
        return parentNode;
    }
}
