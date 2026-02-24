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

import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

public class TransformNifiCommandFactory {

    public static final String TRANSFORM_NIFI = "transform-nifi";

    private static final String COMMAND_DESCRIPTION = "Transform NiFi flow JSON format into MiNifi flow JSON format";

    private final PathInputStreamFactory pathInputStreamFactory;
    private final PathOutputStreamFactory pathOutputStreamFactory;

    public TransformNifiCommandFactory(final PathInputStreamFactory pathInputStreamFactory,
            final PathOutputStreamFactory pathOutputStreamFactory) {
        this.pathInputStreamFactory = pathInputStreamFactory;
        this.pathOutputStreamFactory = pathOutputStreamFactory;
    }

    public ConfigMain.Command create() {
        return new ConfigMain.Command(this::transformNifiToJson, COMMAND_DESCRIPTION);
    }

    private int transformNifiToJson(final String[] args) {
        if (args.length != 3) {
            printTransformUsage();
            return ConfigMain.ERR_INVALID_ARGS;
        }

        final String sourceNiFiJsonPath = args[1];
        final String targetMiNiFiJsonPath = args[2];

        try {
            final RegisteredFlowSnapshot registeredFlowSnapshot = readNifiFlow(sourceNiFiJsonPath);
            final VersionedDataflow versionedDataflow = new VersionedDataflow();
            versionedDataflow.setRootGroup(registeredFlowSnapshot.getFlowContents());
            versionedDataflow.setParameterContexts(new ArrayList<>(registeredFlowSnapshot.getParameterContexts().values()));
            setDefaultValues(versionedDataflow);

            persistFlowJson(versionedDataflow, targetMiNiFiJsonPath);
        } catch (final ConfigTransformException e) {
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

    private RegisteredFlowSnapshot readNifiFlow(final String sourceNiFiJsonPath) throws ConfigTransformException {
        try (InputStream inputStream = pathInputStreamFactory.create(sourceNiFiJsonPath)) {
            final ObjectMapper objectMapper = ObjectMapperUtils.createObjectMapper();
            return objectMapper.readValue(inputStream, RegisteredFlowSnapshot.class);
        } catch (final IOException e) {
            throw new ConfigTransformException("Error when reading NiFi flow json file",
                    ConfigMain.ERR_UNABLE_TO_OPEN_INPUT, e);
        }
    }

    private void persistFlowJson(final VersionedDataflow flow, final String flowJsonPath) throws ConfigTransformException {
        try (OutputStream outputStream = pathOutputStreamFactory.create(flowJsonPath)) {
            final ObjectMapper objectMapper = ObjectMapperUtils.createObjectMapper();
            objectMapper.writeValue(outputStream, flow);
        } catch (final IOException e) {
            throw new ConfigTransformException("Error when persisting flow JSON file: " + flowJsonPath,
                    ConfigMain.ERR_UNABLE_TO_SAVE_CONFIG, e);
        }
    }

    private void setDefaultValues(final VersionedDataflow versionedDataflow) {
        versionedDataflow.setMaxTimerDrivenThreadCount(CorePropertiesSchema.DEFAULT_MAX_CONCURRENT_THREADS);
        setDefaultValues(versionedDataflow.getRootGroup());
    }

    private void setDefaultValues(final VersionedProcessGroup versionedProcessGroup) {
        versionedProcessGroup.getRemoteProcessGroups().forEach(this::setDefaultValues);
        versionedProcessGroup.getProcessGroups().forEach(this::setDefaultValues);
    }

    private void setDefaultValues(final VersionedRemoteProcessGroup versionedRemoteProcessGroup) {
        versionedRemoteProcessGroup.getInputPorts().forEach(this::setDefaultValues);
    }

    private void setDefaultValues(final VersionedRemoteGroupPort versionedRemoteGroupPort) {
        versionedRemoteGroupPort.setScheduledState(ScheduledState.RUNNING);
    }

}
