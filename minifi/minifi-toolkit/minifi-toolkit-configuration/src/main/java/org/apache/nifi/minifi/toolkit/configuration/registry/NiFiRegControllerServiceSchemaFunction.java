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
package org.apache.nifi.minifi.toolkit.configuration.registry;

import org.apache.nifi.minifi.commons.schema.ControllerServiceSchema;
import org.apache.nifi.registry.flow.VersionedControllerService;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.nifi.minifi.commons.schema.common.CollectionUtil.nullToEmpty;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ANNOTATION_DATA_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ID_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROPERTIES_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.TYPE_KEY;

public class NiFiRegControllerServiceSchemaFunction implements Function<VersionedControllerService, ControllerServiceSchema> {

    @Override
    public ControllerServiceSchema apply(final VersionedControllerService versionedControllerService) {
        Map<String, Object> map = new HashMap<>();
        map.put(NAME_KEY, versionedControllerService.getName());
        map.put(ID_KEY, versionedControllerService.getIdentifier());
        map.put(TYPE_KEY, versionedControllerService.getType());

        map.put(PROPERTIES_KEY, new HashMap<>(nullToEmpty(versionedControllerService.getProperties())));

        String annotationData = versionedControllerService.getAnnotationData();
        if(annotationData != null && !annotationData.isEmpty()) {
            map.put(ANNOTATION_DATA_KEY, annotationData);
        }

        return new ControllerServiceSchema(map);
    }
}
