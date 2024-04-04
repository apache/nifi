/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.processors.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JsonUtils {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private JsonUtils() {}

    static String readString(final Path path) throws IOException {
        return Files.readString(path);
    }

    static String toJson(final Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    static String prettyPrint(final Object object) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    static Map<String, Object> readMap(final String json) {
        try {
            return MAPPER.readValue(json, Map.class);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    static List<String> readListOfMapsAsIndividualJson(final String json) {
            return readListOfMaps(json).stream()
                    .map(JsonUtils::prettyPrint)
                    .collect(Collectors.toList());
    }

    static List<Map<String, Object>> readListOfMaps(final String json) {
        try {
            return MAPPER.readValue(json, List.class);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
