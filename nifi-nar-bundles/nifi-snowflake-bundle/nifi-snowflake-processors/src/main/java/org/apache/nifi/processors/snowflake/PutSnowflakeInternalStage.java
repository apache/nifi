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
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes({
        @ReadsAttribute(attribute = "filename", description = "The name of the staged file in the internal stage"),
        @ReadsAttribute(attribute = "path", description = "The relative path to the staged file in the internal stage")
})
@WritesAttributes({
        @WritesAttribute(attribute = ATTRIBUTE_STAGED_FILE_PATH,
                description = "Staged file path")
})
@Tags({"snowflake", "jdbc", "database", "connection"})
@CapabilityDescription("Puts files into a Snowflake internal stage. The internal stage must be created in the Snowflake account beforehand."
        + " This processor can be connected to an StartSnowflakeIngest processor to ingest the file in the internal stage")
@SeeAlso({StartSnowflakeIngest.class, GetSnowflakeIngestStatus.class})
public class PutSnowflakeInternalStage extends AbstractProcessor {

    static final PropertyDescriptor SNOWFLAKE_CONNECTION_PROVIDER = new PropertyDescriptor.Builder()
            .name("snowflake-connection-provider")
            .displayName("Snowflake Connection Provider")
            .description("Specifies the Controller Service to use for creating SQL connections to Snowflake.")
            .identifiesControllerService(SnowflakeConnectionProviderService.class)
            .required(true)
            .build();

    static final PropertyDescriptor INTERNAL_STAGE_NAME = new PropertyDescriptor.Builder()
            .name("internal-stage-name")
            .displayName("Internal Stage Name")
            .description("The name of the internal stage in the Snowflake account to put files into.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles of successful PUT operation")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("For FlowFiles of failed PUT operation")
            .build();

    static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            SNOWFLAKE_CONNECTION_PROVIDER,
            INTERNAL_STAGE_NAME
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
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
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String internalStageName = context.getProperty(INTERNAL_STAGE_NAME)
                .evaluateAttributeExpressions()
                .getValue();
        final SnowflakeConnectionProviderService connectionProviderService =
                context.getProperty(SNOWFLAKE_CONNECTION_PROVIDER)
                        .asControllerService(SnowflakeConnectionProviderService.class);

        final String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        final String relativePath = flowFile.getAttribute(CoreAttributes.PATH.key());
        final String stageRelativePath = "./".equals(relativePath)
                ? ""
                : relativePath;
        try (final InputStream inputStream = session.read(flowFile);
                final SnowflakeConnectionWrapper snowflakeConnection = connectionProviderService.getSnowflakeConnection()) {
            snowflakeConnection.unwrap()
                    .uploadStream(internalStageName, stageRelativePath, inputStream, fileName, false);
        } catch (SQLException e) {
            final String stagedFilePath = stageRelativePath + fileName;
            getLogger().error("Failed to upload FlowFile content to internal Snowflake stage [" + internalStageName + "]. Staged file path [" + stagedFilePath + "]", e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        } catch (IOException e) {
            throw new ProcessException("Failed to read FlowFile content", e);
        }

        final String stagedFileName = stageRelativePath + fileName;
        flowFile = session.putAttribute(flowFile, ATTRIBUTE_STAGED_FILE_PATH, stagedFileName);
        session.transfer(flowFile, REL_SUCCESS);
    }
}
