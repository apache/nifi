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
package org.apache.nifi.registry.client.impl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.bucket.BucketItemType;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.flow.VersionedFlow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BucketItemDeserializer extends StdDeserializer<BucketItem[]> {

    public BucketItemDeserializer() {
        super(BucketItem[].class);
    }

    @Override
    public BucketItem[] deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        final JsonNode arrayNode = jsonParser.getCodec().readTree(jsonParser);

        final List<BucketItem> bucketItems = new ArrayList<>();

        final Iterator<JsonNode> nodeIter = arrayNode.elements();
        while (nodeIter.hasNext()) {
            final JsonNode node = nodeIter.next();

            final String type = node.get("type").asText();
            if (StringUtils.isBlank(type)) {
                throw new IllegalStateException("BucketItem type cannot be null or blank");
            }

            final BucketItemType bucketItemType;
            try {
                bucketItemType = BucketItemType.valueOf(type);
            } catch (Exception e) {
                throw new IllegalStateException("Unknown type for BucketItem: " + type, e);
            }


            switch (bucketItemType) {
                case Flow:
                    final VersionedFlow versionedFlow = jsonParser.getCodec().treeToValue(node, VersionedFlow.class);
                    bucketItems.add(versionedFlow);
                    break;
                case Bundle:
                    final Bundle bundle = jsonParser.getCodec().treeToValue(node, Bundle.class);
                    bucketItems.add(bundle);
                    break;
                default:
                    throw new IllegalStateException("Unknown type for BucketItem: " + bucketItemType);
            }
        }

        return bucketItems.toArray(new BucketItem[bucketItems.size()]);
    }

}
