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

package org.apache.nifi.minifi.toolkit.configuration.dto;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.minifi.commons.schema.RemotePortSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ID_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;

public class RemotePortSchemaFunction implements Function<RemoteProcessGroupPortDTO, RemotePortSchema> {
    @Override
    public RemotePortSchema apply(RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        Map<String, Object> map = new HashMap<>();
        // If a targetId is specified, it takes precedence over the original id
        final String targetId = remoteProcessGroupPortDTO.getTargetId();
        map.put(ID_KEY, StringUtils.isNotBlank(targetId) ? targetId : remoteProcessGroupPortDTO.getId());
        map.put(NAME_KEY, remoteProcessGroupPortDTO.getName());

        map.put(CommonPropertyKeys.COMMENT_KEY, remoteProcessGroupPortDTO.getComments());
        map.put(CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY, remoteProcessGroupPortDTO.getConcurrentlySchedulableTaskCount());
        map.put(CommonPropertyKeys.USE_COMPRESSION_KEY, remoteProcessGroupPortDTO.getUseCompression());
        return new RemotePortSchema(map);
    }
}
