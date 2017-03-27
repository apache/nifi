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

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;

import java.util.Map;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

public interface KafkaTopicDataSetCreator extends DataSetEntityCreator {
    @Override
    default AtlasEntity create(AtlasObjectId objectId, Map<String, String> properties) {
        final Object topic = objectId.getUniqueAttributes().get("topic");
        final AtlasEntity entity = new AtlasEntity();
        entity.setTypeName("kafka_topic");
        entity.setAttribute(ATTR_NAME, topic);
        entity.setAttribute(ATTR_QUALIFIED_NAME, topic);
        entity.setAttribute("topic", topic);
        entity.setAttribute("uri", properties.get("bootstrap.servers"));
        entity.setVersion(1L);
        return entity;
    }
}
