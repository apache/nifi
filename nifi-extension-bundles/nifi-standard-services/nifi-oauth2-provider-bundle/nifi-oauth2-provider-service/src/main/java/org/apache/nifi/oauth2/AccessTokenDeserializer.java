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
package org.apache.nifi.oauth2;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AccessTokenDeserializer extends StdDeserializer<AccessToken> {

    public AccessTokenDeserializer() {
        this(AccessToken.class);
    }

    public AccessTokenDeserializer(final Class<?> vc) {
        super(vc);
    }

    @Override
    public AccessToken deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException {
        final JsonNode node = jp.getCodec().readTree(jp);
        final List<String> knownFields = Arrays.asList("access_token", "refresh_token", "token_type", "expires_in", "scope");

        // Create the AccessToken object
        String accessToken = node.has("access_token") ? node.get("access_token").asText() : null;
        String refreshToken = node.has("refresh_token") ? node.get("refresh_token").asText() : null;
        String tokenType = node.has("token_type") ? node.get("token_type").asText() : null;
        long expiresIn = node.has("expires_in") ? node.get("expires_in").asLong() : 0;
        String scope = node.has("scope") ? node.get("scope").asText() : null;

        AccessToken token = new AccessToken(accessToken, refreshToken, tokenType, expiresIn, scope);

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (!knownFields.contains(field.getKey())) {
                token.setCustomField(field.getKey(), field.getValue().asText());
            }
        }

        return token;
    }

}
