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
package org.apache.nifi.processors.smb;

import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.processor.util.StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbException;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"samba, smb, cifs, files", "fetch"})
@CapabilityDescription("Fetches files from a SMB Share. Designed to be used in tandem with ListSmb.")
@SeeAlso({ListSmb.class, PutSmbFile.class, GetSmbFile.class})
@WritesAttributes({
        @WritesAttribute(attribute = FetchSmb.ERROR_CODE_ATTRIBUTE, description = "The error code returned by SMB when the fetch of a file fails"),
        @WritesAttribute(attribute = FetchSmb.ERROR_MESSAGE_ATTRIBUTE, description = "The error message returned by SMB when the fetch of a file fails")
})
public class FetchSmb extends AbstractProcessor {

    public static final String ERROR_CODE_ATTRIBUTE = "error.code";
    public static final String ERROR_MESSAGE_ATTRIBUTE = "error.message";

    public static final PropertyDescriptor REMOTE_FILE = new PropertyDescriptor
            .Builder().name("remote-file")
            .displayName("Remote File")
            .description(
                    "The full path of the file to be retrieved from the remote server. EL is supported when record reader is not used.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .defaultValue("${path}/${filename}")
            .addValidator(ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();

    public static final PropertyDescriptor SMB_CLIENT_PROVIDER_SERVICE = new Builder()
            .name("smb-client-provider-service")
            .displayName("SMB Client Provider Service")
            .description("Specifies the SMB client provider to use for creating SMB connections.")
            .required(true)
            .identifiesControllerService(SmbClientProviderService.class)
            .build();
    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("A flowfile will be routed here for each successfully fetched File.")
                    .build();
    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description(
                            "A flowfile will be routed here for each File for which fetch was attempted but failed.")
                    .build();
    public static final Relationship REL_INPUT_FAILURE =
            new Relationship.Builder().name("input_failure")
                    .description("The incoming flowfile will be routed here if its content could not be processed.")
                    .build();
    public static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(asList(
            REL_SUCCESS,
            REL_FAILURE,
            REL_INPUT_FAILURE
    )));
    public static final String UNCATEGORIZED_ERROR = "-2";
    public static final String FILE_NAME = "filename";
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description(
                    "Specifies the Controller Service to use for reading incoming NiFi Records. Each record should contain \"identifier\""
                            + " attribute set to the path and name of the file to fetch."
                            + " If not set, the Processor expects as attributes of a separate flowfile for each File to fetch.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = asList(
            SMB_CLIENT_PROVIDER_SERVICE,
            RECORD_READER,
            REMOTE_FILE
    );

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final List<Map<String, String>> attributes;
        try {
            attributes = collectAttributes(context, session, flowFile);
            attributes.stream()
                    .map(attribute -> findFileName(context, attribute))
                    .forEach(this::validateFileName);
        } catch (Exception e) {
            handleInputError(e, session, flowFile);
            return;
        }

        final SmbClientProviderService clientProviderService =
                context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);

        try (SmbClientService client = clientProviderService.getClient()) {
            attributes.forEach(attribute -> fetchAndTransfer(session, context, client, attribute));
            session.remove(flowFile);
        } catch (Exception e) {
            getLogger().error("Couldn't connect to smb due to " + e.getMessage());
            session.rollback();
            FlowFile outFlowFile = session.get();
            outFlowFile = session.putAttribute(outFlowFile, ERROR_CODE_ATTRIBUTE, getErrorCode(e));
            outFlowFile = session.putAttribute(outFlowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());
            session.transfer(outFlowFile, REL_INPUT_FAILURE);
        }

    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    private boolean shouldReadRecords(ProcessContext context) {
        return context.getProperty(RECORD_READER).isSet();
    }

    private String findFileName(ProcessContext context, Map<String, String> attributes) {
        return shouldReadRecords(context) ?
                Stream.of("path", "filename")
                        .map(attributes::get)
                        .filter(Objects::nonNull)
                        .collect(joining("/")) :
                context.getProperty(REMOTE_FILE).evaluateAttributeExpressions(attributes).getValue();
    }

    private List<Map<String, String>> collectAttributes(ProcessContext context, ProcessSession session,
            FlowFile flowFile)
            throws Exception {
        final List<Map<String, String>> result = new LinkedList<>();
        if (shouldReadRecords(context)) {
            final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER)
                    .asControllerService(RecordReaderFactory.class);
            try (InputStream inFlowFile = session.read(flowFile);
                    final RecordReader reader = recordReaderFactory.createRecordReader(flowFile, inFlowFile,
                            getLogger())) {
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    result.add(record.getRawFieldNames().stream()
                            .collect(toMap(identity(), record::getAsString)));
                }
            }
        } else {
            result.add(flowFile.getAttributes());
        }
        return result;
    }

    private void fetchAndTransfer(ProcessSession session, ProcessContext context, SmbClientService client,
            Map<String, String> attributes) {
        FlowFile flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, attributes);
        final String filename = findFileName(context, attributes);
        try {
            flowFile = session.write(flowFile, outputStream -> client.readFile(filename, outputStream));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Couldn't fetch file {} due to {}", filename, e.getMessage());
            flowFile = session.putAttribute(flowFile, ERROR_CODE_ATTRIBUTE, getErrorCode(e));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void handleInputError(Exception exception, ProcessSession session, FlowFile flowFile) {
        if (exception instanceof IOException || exception instanceof MalformedRecordException
                || exception instanceof SchemaNotFoundException) {
            getLogger().error("Failed to read input records {}", flowFile, exception);
        } else {
            getLogger().error("Failed to read input {}", flowFile, exception);
        }
        session.rollback(false);
        flowFile = session.get();
        flowFile = session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE,
                Optional.ofNullable(exception.getMessage()).orElse(exception.getClass().getSimpleName()));
        session.transfer(flowFile, REL_INPUT_FAILURE);
    }

    private void validateFileName(String fileName) {
        if (fileName == null || fileName.isEmpty() || fileName.endsWith("/")) {
            throw new IllegalArgumentException("Couldn't find filename in flowfile");
        }
    }

    private String getErrorCode(Exception exception) {
        return Optional.ofNullable(exception instanceof SmbException ? (SmbException) exception : null)
                .map(SmbException::getErrorCode)
                .map(String::valueOf)
                .orElse(UNCATEGORIZED_ERROR);
    }

}

