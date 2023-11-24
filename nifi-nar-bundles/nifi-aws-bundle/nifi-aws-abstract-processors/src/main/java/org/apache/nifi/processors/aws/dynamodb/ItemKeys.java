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
package org.apache.nifi.processors.aws.dynamodb;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Objects;

/**
 * Utility class to keep a map of keys and flow files
 */
class ItemKeys {

    protected AttributeValue hashKey = AttributeValue.fromS("");
    protected AttributeValue rangeKey = AttributeValue.fromS("");

    public ItemKeys(final AttributeValue hashKey, final AttributeValue rangeKey) {
        if (hashKey != null) {
            this.hashKey = hashKey;
        }

        if (rangeKey != null) {
            this.rangeKey = rangeKey;
        }
    }

    @Override
    public String toString() {
        return "ItemKeys[hashKey=" + hashKey + ", rangeKey=" + rangeKey + "]";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ItemKeys itemKeys = (ItemKeys) o;
        return Objects.equals(hashKey, itemKeys.hashKey) && Objects.equals(rangeKey, itemKeys.rangeKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashKey, rangeKey);
    }
}
