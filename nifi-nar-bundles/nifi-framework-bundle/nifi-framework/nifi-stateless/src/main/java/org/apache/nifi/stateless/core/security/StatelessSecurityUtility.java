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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Map;
import org.apache.nifi.security.util.crypto.CipherUtility;

public class StatelessSecurityUtility {

    public static String getLoggableRepresentationOfSensitiveValue(String sensitiveValue) {
        return CipherUtility.getLoggableRepresentationOfSensitiveValue(sensitiveValue);
    }

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
}
