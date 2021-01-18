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
package org.apache.nifi.stateless.core.security;

import static org.apache.nifi.stateless.runtimes.Program.JSON_FLAG;
import static org.apache.nifi.stateless.runtimes.Program.YARN_JSON_FLAG;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.nifi.security.util.crypto.CipherUtility;
import org.apache.nifi.stateless.runtimes.Program;

public class StatelessSecurityUtility {

    /**
     * Returns a masked value of this input.
     *
     * @param sensitiveValue the provided input
     * @return a securely-hashed, deterministic output value
     */
    public static String getLoggableRepresentationOfSensitiveValue(String sensitiveValue) {
        return CipherUtility.getLoggableRepresentationOfSensitiveValue(sensitiveValue);
    }

    /**
     * Returns a String representation of this JSON object with a masked value for any sensitive parameters.
     *
     * @param json the JSON object
     * @return the string contents with sensitive parameters masked
     */
    public static String getLoggableRepresentationOfJsonObject(final JsonObject json) {
        JsonObject localJson = null;
        boolean maskedParams = false;

        if (json.has("parameters")) {
            JsonObject parameters = json.getAsJsonObject("parameters");
            for (Map.Entry<String, JsonElement> e : parameters.entrySet()) {
                if (e.getValue().isJsonObject()) {
                    JsonObject paramDescriptorMap = (JsonObject) e.getValue();
                    if (paramDescriptorMap.has("sensitive") && paramDescriptorMap.getAsJsonPrimitive("sensitive").getAsBoolean()) {
                        maskedParams = true;
                        if (localJson == null) {
                            localJson = json.deepCopy();
                        }
                        // Point the PDM reference to the copied JSON so we don't modify the parameter internals
                        paramDescriptorMap = localJson.getAsJsonObject("parameters").getAsJsonObject(e.getKey()).getAsJsonObject();
                        // This parameter is sensitive; replace its "value" with the masked value
                        String maskedValue = getLoggableRepresentationOfSensitiveValue(paramDescriptorMap.getAsJsonPrimitive("value").getAsString());
                        paramDescriptorMap.addProperty("value", maskedValue);
                        localJson.getAsJsonObject("parameters").add(e.getKey(), paramDescriptorMap);
                    }
                }
            }
        }

        // If no params were changed, return the original JSON
        if (!maskedParams) {
            return json.toString();
        } else {
            return localJson.toString();
        }
    }

    /**
     * Returns a String containing the provided arguments, with any JSON objects having their
     * sensitive values masked. Elements are joined with {@code ,}. If {@code isVerbose} is
     * {@code false}, elides the JSON entirely.
     *
     * @param args      the list of arguments
     * @param isVerbose if {@code true}, will print the complete JSON value
     * @return the masked string response
     */
    public static String formatArgs(String[] args, boolean isVerbose) {
        List<String> argsList = new ArrayList<>(Arrays.asList(args));
        int jsonIndex = determineJsonIndex(argsList);

        if (jsonIndex != -1) {
            if (isVerbose) {
                JsonObject json = new JsonParser().parse(argsList.get(jsonIndex)).getAsJsonObject();
                String maskedJson = getLoggableRepresentationOfJsonObject(json);
                argsList.add(jsonIndex, maskedJson);
            } else {
                argsList.add(jsonIndex, "{...json...}");
            }
            argsList.remove(jsonIndex + 1);
        }

        return String.join(",", argsList);
    }

    /**
     * Returns a String containing the JSON object with any sensitive values masked.
     *
     * @param json the JSON object
     * @return a masked string
     */
    public static String formatJson(JsonObject json) {
        return StatelessSecurityUtility.getLoggableRepresentationOfJsonObject(json);
    }

    /**
     * Returns the index of the JSON string in this list (checks {@link Program#JSON_FLAG} first, then {@link Program#YARN_JSON_FLAG}). Returns -1 if no JSON is present.
     *
     * @param argsList the list of arguments
     * @return the index of the JSON element or -1
     */
    public static int determineJsonIndex(List<String> argsList) {
        int jsonIndex = -1;
        if (argsList.contains(JSON_FLAG)) {
            jsonIndex = determineJsonIndex(argsList, JSON_FLAG);
        } else if (argsList.contains(YARN_JSON_FLAG)) {
            jsonIndex = determineJsonIndex(argsList, YARN_JSON_FLAG);
        }
        return jsonIndex;
    }

    /**
     * Returns the index of the JSON string in this list for the given flag. Returns -1 if no JSON is present.
     *
     * @param argsList the list of arguments
     * @param flag     either {@link Program#JSON_FLAG} or {@link Program#YARN_JSON_FLAG}
     * @return the index of the JSON element or -1
     */
    public static int determineJsonIndex(List<String> argsList, String flag) {
        // One of the arguments is a JSON string
        int flagIndex = argsList.indexOf(flag);
        return flagIndex >= 0 ? flagIndex + 1 : -1;
    }

    /**
     * Returns a masked String result given the input if sensitive; the input intact if not.
     *
     * @param input the input string
     * @return masked result if input is sensitive, input otherwise
     */
    public static String sanitizeString(String input) {
        if (isSensitive(input)) {
            return StatelessSecurityUtility.getLoggableRepresentationOfSensitiveValue(input);
        } else {
            return input;
        }
    }

    /**
     * Returns {@code true} if the provided {@code input} is determined to be a sensitive value that
     * needs masking before output. This method uses a series of regular expressions to define common
     * keywords like {@code secret} or {@code password} that indicate a sensitive value.
     *
     * @param input the input string
     * @return true if the value should be masked
     */
    public static boolean isSensitive(String input) {
        return input != null && Program.SENSITIVE_INDICATORS.stream().anyMatch(indicator -> input.toLowerCase().matches(".*" + indicator + ".*"));
    }
}
