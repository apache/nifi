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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.utils.StagedFileWrapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes({
        @ReadsAttribute(attribute = ATTRIBUTE_STAGED_FILE_PATH, description = "The path to the file in the stage")
})
@Tags({"snowflake", "snowpipe", "ingest"})
@CapabilityDescription("Ingest files in a Snowflake stage. The stage must be created in the Snowflake account beforehand."
        + " The result of the ingestion is not available immediately, so this processor can be connected to an"
        + " GetSnowflakeIngestStatus processor to wait for the results")
@SeeAlso({PutSnowflakeInternalStage.class, GetSnowflakeIngestStatus.class})
public class StartSnowflakeIngest extends AbstractProcessor {

    static final PropertyDescriptor INGEST_MANAGER_PROVIDER = new PropertyDescriptor.Builder()
            .name("ingest-manager-provider")
            .displayName("Ingest Manager Provider")
            .description("Ingest manager provider")
            .identifiesControllerService(SnowflakeIngestManagerProviderService.class)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles of successful ingest request")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("For FlowFiles of failed ingest request")
            .build();

    static final List<PropertyDescriptor> PROPERTIES = Collections.singletonList(
            INGEST_MANAGER_PROVIDER
    );

    static final Set<Relationship> RELATIONSHIPS;

    static {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

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
        final SnowflakeIngestManagerProviderService ingestManagerProviderService =
                context.getProperty(INGEST_MANAGER_PROVIDER)
                        .asControllerService(SnowflakeIngestManagerProviderService.class);
        final FlowFile flowFile = session.get();
        final SimpleIngestManager snowflakeIngestManager = ingestManagerProviderService.getIngestManager();
        final String stagedFileName = flowFile.getAttribute(ATTRIBUTE_STAGED_FILE_PATH);
        final StagedFileWrapper stagedFile = new StagedFileWrapper(stagedFileName);
        try {
            snowflakeIngestManager.ingestFile(stagedFile, null);
        } catch (URISyntaxException | IOException  e) {
            throw new ProcessException(String.format("Failed to ingest Snowflake file [%s]", stagedFileName), e);
        } catch (IngestResponseException e) {
            getLogger().error("Failed to ingest Snowflake file [" + stagedFileName + "]", e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }
        session.transfer(flowFile, REL_SUCCESS);
    }
}
