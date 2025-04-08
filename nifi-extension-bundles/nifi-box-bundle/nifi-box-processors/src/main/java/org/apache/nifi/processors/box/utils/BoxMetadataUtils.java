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
package org.apache.nifi.processors.box.utils;

import com.box.sdk.Metadata;
import com.eclipsesource.json.JsonValue;

import java.math.BigDecimal;
import java.util.Map;

public class BoxMetadataUtils {

    /**
     * Parses a JsonValue and returns the appropriate Java object.
     * Box does not allow exponential notation in metadata values, so we need to handle
     * special number formats. For numbers containing decimal points or exponents, we try to
     * convert them to BigDecimal first for precise representation. If that fails, we
     * fall back to double, which might lose precision but allows the processing to continue.
     *
     * @param jsonValue The JsonValue to parse.
     * @return The parsed Java object.
     */
    public static Object parseJsonValue(final JsonValue jsonValue) {
        if (jsonValue == null) {
            return null;
        }
        if (jsonValue.isString()) {
            return jsonValue.asString();
        } else if (jsonValue.isNumber()) {
            final String numberString = jsonValue.toString();
            if (numberString.contains(".") || numberString.toLowerCase().contains("e")) {
                try {
                    return (new BigDecimal(numberString)).toPlainString();
                } catch (final NumberFormatException e) {
                    return jsonValue.asDouble();
                }
            } else {
                try {
                    return jsonValue.asLong();
                } catch (final NumberFormatException e) {
                    return (new BigDecimal(numberString)).toPlainString();
                }
            }
        } else if (jsonValue.isBoolean()) {
            return jsonValue.asBoolean();
        }
        // Fallback: return the string representation.
        return jsonValue.toString();
    }

    /**
     * Processes Box metadata instance and populates the provided map with the default fields.
     *
     * @param fileId         The ID of the file.
     * @param metadata       The Box metadata instance.
     * @param instanceFields The map to populate with metadata fields.
     */
    public static void processBoxMetadataInstance(final String fileId,
                                                  final Metadata metadata,
                                                  final Map<String, Object> instanceFields) {
        instanceFields.put("$id", metadata.getID());
        instanceFields.put("$type", metadata.getTypeName());
        instanceFields.put("$parent", "file_" + fileId); // match the Box API format
        instanceFields.put("$template", metadata.getTemplateName());
        instanceFields.put("$scope", metadata.getScope());

        for (final String fieldName : metadata.getPropertyPaths()) {
            final JsonValue jsonValue = metadata.getValue(fieldName);
            if (jsonValue != null) {
                final String cleanFieldName = fieldName.startsWith("/") ? fieldName.substring(1) : fieldName;
                final Object fieldValue = parseJsonValue(jsonValue);
                instanceFields.put(cleanFieldName, fieldValue);
            }
        }
    }
}
