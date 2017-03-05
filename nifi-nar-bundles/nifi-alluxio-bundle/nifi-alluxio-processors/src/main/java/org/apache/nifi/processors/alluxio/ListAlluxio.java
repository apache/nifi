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
package org.apache.nifi.processors.alluxio;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;

@Tags({"alluxio", "tachyon", "list", "file"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("!!EXPERIMENTAL!! User must be aware that due to some limitations, it is currently impossible to exchange "
        + "with multiple Alluxio clusters. Besides once Alluxio client has been initialized, modifications will only be taken into "
        + "account after NiFi is restarted (JVM restart). List files for a given URI using Alluxio client.")
@SeeAlso({FetchAlluxio.class, PutAlluxio.class})
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
        + "This allows the processor to list only files that have been added or modified after this date the next time that the processor is run. "
        + "This processor is meant to be run on the primary node only.")
public class ListAlluxio extends AbstractAlluxioProcessor {

    public static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
            .name("alluxio-uri")
            .displayName("URI")
            .description("The path to use when listing files. Example: /path")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Each listed file will create a FlowFile that will be routed to this relationship")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;
    private final static String LAST_TIMESTAMP_STATE_KEY = "lastTimestamp";

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(URI);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String uri = context.getProperty(URI).evaluateAttributeExpressions().getValue();

        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap;
        final Map<String, String> stateMapProperties;
        try {
            stateMap = stateManager.getState(Scope.CLUSTER);
            stateMapProperties = new HashMap<>(stateMap.toMap());
        } catch (IOException ioe) {
            throw new ProcessException(ioe);
        }

        long lastTimestamp = -1;
        long newTimestamp = -1;
        String lastTimestampInState = stateMapProperties.get(LAST_TIMESTAMP_STATE_KEY);
        if (!StringUtils.isEmpty(lastTimestampInState)) {
            lastTimestamp = Long.parseLong(lastTimestampInState);
        }

        try {
            List<URIStatus> list = fileSystem.get().listStatus(new AlluxioURI(uri));

            for (URIStatus uriStatus : list) {
                if(uriStatus.getLastModificationTimeMs() > lastTimestamp) {
                    FlowFile flowFile = session.create();
                    flowFile = updateFlowFile(uriStatus, flowFile, session);
                    session.getProvenanceReporter().receive(flowFile, uri + "/" + uriStatus.getName());
                    session.transfer(flowFile, REL_SUCCESS);
                    newTimestamp = uriStatus.getLastModificationTimeMs();
                }
            }
        } catch (FileDoesNotExistException e) {
            getLogger().error("File does not exist: " + e.getMessage(), e);;
            context.yield();
        } catch (IOException e) {
            getLogger().error("IO Exception while fetching data: " + e.getMessage(), e);;
            context.yield();
        } catch (AlluxioException e) {
            getLogger().error("Exception raised by Alluxio: " + e.getMessage(), e);;
            context.yield();
        } finally {
            try {
                if(newTimestamp != -1) {
                    stateMapProperties.put(LAST_TIMESTAMP_STATE_KEY, Long.toString(newTimestamp));
                    // Update state
                    if (stateMap.getVersion() == -1) {
                        stateManager.setState(stateMapProperties, Scope.CLUSTER);
                    } else {
                        stateManager.replace(stateMap, stateMapProperties, Scope.CLUSTER);
                    }
                }
            } catch (IOException e) {
                throw new ProcessException(e);
            }
        }
    }

}
