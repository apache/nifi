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
package org.apache.nifi.c2.client.service;

import static java.util.Collections.singletonList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.apache.nifi.c2.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowIdHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowIdHolder.class);
    private static final String FLOW_IDENTIFIER_FILENAME = "flow-identifier";

    private volatile String flowId;
    private final String configDirectoryName;

    public FlowIdHolder(String configDirectoryName) {
        this.configDirectoryName = configDirectoryName;
        this.flowId = readFlowId();
    }

    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
        persistFlowId(flowId);
    }

    private void persistFlowId(String flowId) {
        File flowIdFile = new File(configDirectoryName, FLOW_IDENTIFIER_FILENAME);
        try {
            FileUtils.ensureDirectoryExistAndCanAccess(flowIdFile.getParentFile());
            saveFlowId(flowIdFile, flowId);
        } catch (IOException e) {
            LOGGER.error("Persisting Flow [{}] failed", flowId, e);
        }
    }

    private void saveFlowId(File flowUpdateInfoFile, String flowId) {
        try {
            Files.write(flowUpdateInfoFile.toPath(), singletonList(flowId));
        } catch (IOException e) {
            LOGGER.error("Writing Flow [{}] failed", flowId, e);
        }
    }

    private String readFlowId() {
        File flowUpdateInfoFile = new File(configDirectoryName, FLOW_IDENTIFIER_FILENAME);
        String flowId = null;
        if (flowUpdateInfoFile.exists()) {
            try {
                List<String> fileLines = Files.readAllLines(flowUpdateInfoFile.toPath());
                if (fileLines.size() != 1) {
                    throw new IllegalStateException(String.format("The file %s for the persisted flow id has the incorrect format.", flowUpdateInfoFile));
                }
                flowId = fileLines.get(0);
            } catch (IOException e) {
                throw new IllegalStateException(String.format("Could not read file %s for persisted flow id.", flowUpdateInfoFile), e);
            }
        }
        return flowId;
    }
}
