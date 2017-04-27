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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class PublishKafka implements Egress, KafkaTopicDataSetCreator {

    @Override
    public Set<AtlasObjectId> getOutputs(Map<String, String> properties, AtlasVariables atlasVariables) {
        final String topic = properties.get("topic");
        if (topic == null || topic.isEmpty() || ExpressionUtils.isExpressionLanguagePresent(topic)) {
            return null;
        }

        // 'topic' is an unique attribute for 'kafka_topic' type.
        return Collections.singleton(new AtlasObjectId("kafka_topic", "topic", topic));
    }

}
