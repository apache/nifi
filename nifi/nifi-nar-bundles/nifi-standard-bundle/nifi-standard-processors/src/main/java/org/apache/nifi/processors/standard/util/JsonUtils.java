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
package org.apache.nifi.processors.standard.util;

import net.minidev.json.JSONValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Provides utilities for interacting with JSON elements
 *
 * @see <a href="http://json.org">http://json.org</a>
 */
public class JsonUtils {

    /**
     * JSONValue#isValidJson is permissive to the degree of the Smart JSON definition, accordingly a strict JSON approach
     * is preferred in determining whether or not a document is valid.
     * Performs a validation of the provided stream according to RFC 4627 as implemented by {@link net.minidev.json.parser.JSONParser#MODE_RFC4627}
     *
     * @param inputStream of content to be validated as JSON
     * @return true, if the content is valid within the bounds of the strictness specified; false otherwise
     * @throws IOException
     */
    public static boolean isValidJson(InputStream inputStream) throws IOException {
        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream)) {
            return JSONValue.isValidJsonStrict(inputStreamReader);
        }
    }

}
