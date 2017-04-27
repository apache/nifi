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
package org.apache.nifi.atlas.processors;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.atlas.AtlasVariables;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ConsumeKafka implements Ingress, KafkaTopicDataSetCreator {

    private Set<String> getTopics(Map<String, String> properties) {
        final String topicString = properties.get("topic");

        if (topicString == null || topicString.isEmpty() || ExpressionUtils.isExpressionLanguagePresent(topicString)) {
            return Collections.emptySet();
        }

        return Arrays.stream(topicString.split(","))
                .filter(s -> s != null && !s.isEmpty()).collect(Collectors.toSet());
    }

    @Override
    public Set<AtlasObjectId> getInputs(Map<String, String> properties, AtlasVariables atlasVariables) {
        final Set<AtlasObjectId> objectIds = new HashSet<>();
        for (String topic : getTopics(properties)) {
            // 'topic' is an unique attribute for 'kafka_topic' type.
            objectIds.add(new AtlasObjectId("kafka_topic", "topic", topic));
        }

        return objectIds;
    }

}
