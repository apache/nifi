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

import com.box.sdkgen.schemas.metadatafull.MetadataFull;

import java.math.BigDecimal;
import java.util.Map;

public class BoxMetadataUtils {

    /**
     * Parses an Object value and returns the appropriate Java object.
     * Box does not allow exponential notation in metadata values, so we need to handle
     * special number formats. For numbers containing decimal points or exponents, we try to
     * convert them to BigDecimal first for precise representation. If that fails, we
     * fall back to double, which might lose precision but allows the processing to continue.
     *
     * @param value The Object value to parse.
     * @return The parsed Java object.
     */
    public static Object parseValue(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return value;
        } else if (value instanceof Number) {
            final String numberString = value.toString();
            if (numberString.contains(".") || numberString.toLowerCase().contains("e")) {
                try {
                    return (new BigDecimal(numberString)).toPlainString();
                } catch (final NumberFormatException e) {
                    return ((Number) value).doubleValue();
                }
            } else {
                try {
                    return ((Number) value).longValue();
                } catch (final NumberFormatException e) {
                    return (new BigDecimal(numberString)).toPlainString();
                }
            }
        } else if (value instanceof Boolean) {
            return value;
        }
        // Fallback: return the string representation.
        return value.toString();
    }

    /**
     * Processes Box metadata instance and populates the provided map with the default fields.
     *
     * @param fileId         The ID of the file.
     * @param scope          The scope of the metadata (e.g., "enterprise", "global").
     * @param templateKey    The template key of the metadata.
     * @param metadata       The Box metadata instance.
     * @param instanceFields The map to populate with metadata fields.
     */
    public static void processBoxMetadataInstance(final String fileId,
                                                  final String scope,
                                                  final String templateKey,
                                                  final MetadataFull metadata,
                                                  final Map<String, Object> instanceFields) {
        instanceFields.put("$id", metadata.getId());
        instanceFields.put("$type", metadata.getType());
        instanceFields.put("$parent", "file_" + fileId); // match the Box API format
        instanceFields.put("$template", templateKey);
        instanceFields.put("$scope", scope);
        instanceFields.put("$typeVersion", metadata.getTypeVersion());
        instanceFields.put("$canEdit", metadata.getCanEdit());

        // Process extra data (custom fields)
        final Map<String, Object> extraData = metadata.getExtraData();
        if (extraData != null) {
            for (final Map.Entry<String, Object> entry : extraData.entrySet()) {
                final String fieldName = entry.getKey();
                // Skip system fields that start with $
                if (!fieldName.startsWith("$")) {
                    final Object fieldValue = parseValue(entry.getValue());
                    instanceFields.put(fieldName, fieldValue);
                }
            }
        }
    }
}
