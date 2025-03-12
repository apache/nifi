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

package org.apache.nifi.minifi.toolkit.configuration.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.minifi.toolkit.configuration.ConfigMain;
import org.apache.nifi.minifi.toolkit.configuration.ConfigTransformException;
import org.apache.nifi.minifi.toolkit.configuration.PathInputStreamFactory;
import org.apache.nifi.minifi.toolkit.configuration.PathOutputStreamFactory;
import org.apache.nifi.minifi.toolkit.schema.CorePropertiesSchema;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TransformNifiCommandFactory {

    public static final String TRANSFORM_NIFI = "transform-nifi";

    private static final String COMMAND_DESCRIPTION = "Transform NiFi flow JSON format into MiNifi flow JSON format";

    private final PathInputStreamFactory pathInputStreamFactory;
    private final PathOutputStreamFactory pathOutputStreamFactory;

    public TransformNifiCommandFactory(PathInputStreamFactory pathInputStreamFactory,
            PathOutputStreamFactory pathOutputStreamFactory) {
        this.pathInputStreamFactory = pathInputStreamFactory;
        this.pathOutputStreamFactory = pathOutputStreamFactory;
    }

    public ConfigMain.Command create() {
        return new ConfigMain.Command(this::transformNifiToJson, COMMAND_DESCRIPTION);
    }

    private int transformNifiToJson(String[] args) {
        if (args.length != 3) {
            printTransformUsage();
            return ConfigMain.ERR_INVALID_ARGS;
        }

        String sourceNiFiJsonPath = args[1];
        String targetMiNiFiJsonPath = args[2];

        try {
            RegisteredFlowSnapshot registeredFlowSnapshot = readNifiFlow(sourceNiFiJsonPath);
            VersionedDataflow versionedDataflow = new VersionedDataflow();
            versionedDataflow.setRootGroup(registeredFlowSnapshot.getFlowContents());
            versionedDataflow.setParameterContexts(new ArrayList<>(registeredFlowSnapshot.getParameterContexts().values()));
            setDefaultValues(versionedDataflow);

            persistFlowJson(versionedDataflow, targetMiNiFiJsonPath);
        } catch (ConfigTransformException e) {
            System.out.println("Unable to convert NiFi JSON to MiNiFi flow JSON: " + e);
            return e.getErrorCode();
        }
        return ConfigMain.SUCCESS;
    }

    private void printTransformUsage() {
        System.out.println("transform Nifi Usage:");
        System.out.println();
        System.out.println(" transform-nifi SOURCE_NIFI_JSON_FLOW_FILE TARGET_MINIFI_JSON_FLOW_FILE");
        System.out.println();
    }

    private RegisteredFlowSnapshot readNifiFlow(String sourceNiFiJsonPath) throws ConfigTransformException {
        try (InputStream inputStream = pathInputStreamFactory.create(sourceNiFiJsonPath)) {
            ObjectMapper objectMapper = ObjectMapperUtils.createObjectMapper();
            return objectMapper.readValue(inputStream, RegisteredFlowSnapshot.class);
        } catch (IOException e) {
            throw new ConfigTransformException("Error when reading NiFi flow json file",
                    ConfigMain.ERR_UNABLE_TO_OPEN_INPUT, e);
        }
    }

    private void persistFlowJson(VersionedDataflow flow, String flowJsonPath) throws ConfigTransformException {
        try (OutputStream outputStream = pathOutputStreamFactory.create(flowJsonPath)) {
            ObjectMapper objectMapper = ObjectMapperUtils.createObjectMapper();
            objectMapper.writeValue(outputStream, flow);
        } catch (IOException e) {
            throw new ConfigTransformException("Error when persisting flow JSON file: " + flowJsonPath,
                    ConfigMain.ERR_UNABLE_TO_SAVE_CONFIG, e);
        }
    }

    private void setDefaultValues(VersionedDataflow versionedDataflow) {
        versionedDataflow.setMaxTimerDrivenThreadCount(CorePropertiesSchema.DEFAULT_MAX_CONCURRENT_THREADS);
        setDefaultValues(versionedDataflow.getRootGroup());
    }

    private void setDefaultValues(VersionedProcessGroup versionedProcessGroup) {
        versionedProcessGroup.getRemoteProcessGroups().forEach(this::setDefaultValues);
        versionedProcessGroup.getProcessGroups().forEach(this::setDefaultValues);
    }

    private void setDefaultValues(VersionedRemoteProcessGroup versionedRemoteProcessGroup) {
        versionedRemoteProcessGroup.getInputPorts().forEach(this::setDefaultValues);
    }

    private void setDefaultValues(VersionedRemoteGroupPort versionedRemoteGroupPort) {
        versionedRemoteGroupPort.setScheduledState(ScheduledState.RUNNING);
    }


}
