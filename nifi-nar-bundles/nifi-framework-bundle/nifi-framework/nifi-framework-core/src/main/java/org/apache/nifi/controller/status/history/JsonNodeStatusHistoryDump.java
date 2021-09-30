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
package org.apache.nifi.controller.status.history;

import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;

import java.io.IOException;
import java.io.OutputStream;

final class JsonNodeStatusHistoryDump implements StatusHistoryDump {

    private final StatusHistory nodeStatusHistory;

    JsonNodeStatusHistoryDump(final StatusHistory nodeStatusHistory) {
        this.nodeStatusHistory = nodeStatusHistory;
    }

    @Override
    public void writeTo(final OutputStream out) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
        prettyPrinter.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
        final StatusHistoryDTO statusHistoryDTO = StatusHistoryUtil.createStatusHistoryDTO(nodeStatusHistory);
        objectMapper.writer(prettyPrinter).writeValue(out, statusHistoryDTO);
    }
}
