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
package org.apache.nifi.processors.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxFile;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "fetch"})
@CapabilityDescription("Fetches files from a Box Folder. Designed to be used in tandem with ListBoxFile.")
@SeeAlso({ListBoxFile.class})
@WritesAttributes({
    @WritesAttribute(attribute = FetchBoxFile.ERROR_CODE_ATTRIBUTE, description = "The error code returned by Box when the fetch of a file fails"),
    @WritesAttribute(attribute = FetchBoxFile.ERROR_MESSAGE_ATTRIBUTE, description = "The error message returned by Box when the fetch of a file fails")
})
public class FetchBoxFile extends AbstractProcessor {
    public static final String ERROR_CODE_ATTRIBUTE = "error.code";
    public static final String ERROR_MESSAGE_ATTRIBUTE = "error.message";

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor
        .Builder().name("box-file-id")
        .displayName("File ID")
        .description("The ID of the File to fetch")
        .required(true)
        .defaultValue("${box.id}")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final Relationship REL_SUCCESS =
        new Relationship.Builder()
            .name("success")
            .description("A FlowFile will be routed here for each successfully fetched File.")
            .build();

    public static final Relationship REL_FAILURE =
        new Relationship.Builder().name("failure")
            .description("A FlowFile will be routed here for each File for which fetch was attempted but failed.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
        BoxClientService.BOX_CLIENT_SERVICE,
        FILE_ID
    ));

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS,
        REL_FAILURE
    )));

    private volatile BoxAPIConnection boxAPIConnection;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        BoxClientService boxClientService = context.getProperty(BoxClientService.BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);

        boxAPIConnection = boxClientService.getBoxApiConnection();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String fileId = context.getProperty(FILE_ID).evaluateAttributeExpressions(flowFile).getValue();
        FlowFile outFlowFile = flowFile;
        try {
            outFlowFile = fetchFile(fileId, session, outFlowFile);

            session.transfer(outFlowFile, REL_SUCCESS);
        } catch (BoxAPIResponseException e) {
            handleErrorResponse(session, fileId, flowFile, e);
        } catch (Exception e) {
            handleUnexpectedError(session, flowFile, fileId, e);
        }
    }

    FlowFile fetchFile(String fileId, ProcessSession session, FlowFile outFlowFile) {
        BoxFile boxFile = new BoxFile(boxAPIConnection, fileId);

        outFlowFile = session.write(outFlowFile, outputStream -> boxFile.download(outputStream));

        return outFlowFile;
    }

    private void handleErrorResponse(ProcessSession session, String fileId, FlowFile outFlowFile, BoxAPIResponseException e) {
        getLogger().error("Couldn't fetch file with id '{}'", fileId, e);

        outFlowFile = session.putAttribute(outFlowFile, ERROR_CODE_ATTRIBUTE, "" + e.getResponseCode());
        outFlowFile = session.putAttribute(outFlowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());

        session.transfer(outFlowFile, REL_FAILURE);
    }

    private void handleUnexpectedError(ProcessSession session, FlowFile flowFile, String fileId, Exception e) {
        getLogger().error("Unexpected error while fetching and processing file with id '{}'", fileId, e);

        flowFile = session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());

        session.transfer(flowFile, REL_FAILURE);
    }
}
