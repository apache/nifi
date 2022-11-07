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

package org.apache.nifi.processors.snowflake;

import static org.apache.nifi.processors.snowflake.common.Attributes.ATTRIBUTE_STAGED_FILE_PATH;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.HistoryResponse.FileEntry;
import net.snowflake.ingest.connection.IngestResponseException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Stateful(scopes = Scope.CLUSTER,
        description = "The 'begin mark' from the response of a history request is stored to keep track of already requested history time range.")
@DefaultSettings(penaltyDuration = "5 sec")
@ReadsAttributes({
        @ReadsAttribute(attribute = ATTRIBUTE_STAGED_FILE_PATH, description = "Staged file path")
})
@Tags({"snowflake", "snowpipe", "ingest", "history"})
@CapabilityDescription("Waits until a file in a Snowflake stage is ingested. The stage must be created in the Snowflake account beforehand."
        + " This processor is usually connected to an upstream StartSnowflakeIngest processor to make sure that the file is ingested")
@SeeAlso({StartSnowflakeIngest.class, PutSnowflakeInternalStage.class})
public class GetSnowflakeIngestStatus extends AbstractProcessor {

    static final PropertyDescriptor INGEST_MANAGER_PROVIDER = new PropertyDescriptor.Builder()
            .name("ingest-manager-provider")
            .displayName("Ingest Manager Provider")
            .description("Specifies the Controller Service to use for ingesting Snowflake staged files.")
            .identifiesControllerService(SnowflakeIngestManagerProviderService.class)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles of successful ingestion")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("For FlowFiles of failed ingestion")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("For FlowFiles whose file is still not ingested. These FlowFiles should be routed back to this processor to try again later")
            .build();

    static final List<PropertyDescriptor> PROPERTIES = Collections.singletonList(
            INGEST_MANAGER_PROVIDER
    );

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_RETRY,
            REL_FAILURE
    )));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String stagedFilePath = flowFile.getAttribute(ATTRIBUTE_STAGED_FILE_PATH);
        if (stagedFilePath == null) {
            getLogger().error("Missing required attribute [\"" + ATTRIBUTE_STAGED_FILE_PATH + "\"] for FlowFile");
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        final SnowflakeIngestManagerProviderService ingestManagerProviderService =
                context.getProperty(INGEST_MANAGER_PROVIDER)
                        .asControllerService(SnowflakeIngestManagerProviderService.class);
        final String beginMarkKey = stagedFilePath + ".begin.mark";
        final StateManager stateManager = StateManager.create(beginMarkKey, session);
        final String beginMark = stateManager.getBeginMark();
        final HistoryResponse historyResponse;
        try {
            final SimpleIngestManager snowflakeIngestManager = ingestManagerProviderService.getIngestManager();
            historyResponse = snowflakeIngestManager.getHistory(null, null, beginMark);
        } catch (URISyntaxException | IOException e) {
            throw new ProcessException("Failed to get Snowflake ingest history for staged file [" + stagedFilePath + "]", e);
        } catch (IngestResponseException e) {
            getLogger().error("Failed to get Snowflake ingest history for staged file [" + stagedFilePath + "]", e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        final Optional<FileEntry> fileEntry = Optional.ofNullable(historyResponse.files)
                .flatMap(files -> files.stream()
                        .filter(entry -> entry.getPath().equals(stagedFilePath) && entry.isComplete())
                        .findFirst());

        if (!fileEntry.isPresent()) {
            stateManager.saveBeginMarkToState(historyResponse.getNextBeginMark());
            session.transfer(session.penalize(flowFile), REL_RETRY);
            return;
        }

        stateManager.removeBeginMarkFromState();
        if (fileEntry.get().getErrorsSeen() > 0) {
            getLogger().error("Failed to ingest file [" + stagedFilePath + "] in Snowflake stage via pipe [" + ingestManagerProviderService.getPipeName() + "]."
                    + " Error: " + fileEntry.get().getFirstError());
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }
        session.transfer(flowFile, REL_SUCCESS);

    }

    private static class StateManager {

        private final StateMap stateMap;
        private final String beginMarkKey;
        private final ProcessSession session;

        public StateManager(final StateMap stateMap, final String beginMarkKey, final ProcessSession session) {
            this.stateMap = stateMap;
            this.beginMarkKey = beginMarkKey;
            this.session = session;
        }

        public static StateManager create(final String beginMarkKey, final ProcessSession session) {
            try {
                return new StateManager(session.getState(Scope.CLUSTER), beginMarkKey, session);
            } catch (IOException e) {
                throw new ProcessException("Failed to get state", e);
            }
        }

        public String getBeginMark() {
            return stateMap.get(beginMarkKey);
        }

        public void saveBeginMarkToState(final String newBeginMark) {
            final Map<String, String> newState = new HashMap<>(stateMap.toMap());
            newState.put(beginMarkKey, newBeginMark);
            try {
                session.setState(newState, Scope.CLUSTER);
            } catch (IOException e) {
                throw new ProcessException("Failed to save new begin mark to state", e);
            }
        }

        public void removeBeginMarkFromState() {
            final Map<String, String> newState = new HashMap<>(stateMap.toMap());
            newState.remove(beginMarkKey);
            try {
                session.setState(newState, Scope.CLUSTER);
            } catch (IOException e) {
                throw new ProcessException("Failed to remove begin mark key from state", e);
            }
        }
    }
}
