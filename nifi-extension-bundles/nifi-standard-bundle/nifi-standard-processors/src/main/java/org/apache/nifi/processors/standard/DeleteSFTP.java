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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"remote", "remove", "delete", "sftp"})
@CapabilityDescription("Deletes a file residing on an SFTP server.")
@UseCase(
        description = "Delete source file only after its processing completed",
        configuration = """
                Retrieve a file residing on an SFTP server, e.g. using 'ListSFTP' and 'FetchSFTP'.
                Process the file using any combination of processors.
                Store the resulting file to a destination, e.g. using 'PutFile'.
                Using 'DeleteSFTP', delete the file residing on an SFTP server only after the result has been stored.
                """
)
public class DeleteSFTP extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles, for which an existing file has been deleted, are routed to this relationship")
            .build();
    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("All FlowFiles, for which the file to delete did not exist, are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles, for which an existing file could not be deleted, are routed to this relationship")
            .build();

    private final static Set<Relationship> relationships = Set.of(REL_SUCCESS, REL_NOT_FOUND, REL_FAILURE);

    public static final PropertyDescriptor DIRECTORY_PATH = new PropertyDescriptor.Builder()
            .name("Directory Path")
            .displayName("Directory Path")
            .description("The path to the directory the file to delete is located in.")
            .required(true)
            .defaultValue("${" + CoreAttributes.PATH.key() + "}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
            .name("Filename")
            .displayName("Filename")
            .description("The name of the file to delete.")
            .required(true)
            .defaultValue("${" + CoreAttributes.FILENAME.key() + "}")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private final static List<PropertyDescriptor> properties = List.of(
            DIRECTORY_PATH,
            FILENAME,
            SFTPTransfer.HOSTNAME,
            SFTPTransfer.PORT,
            SFTPTransfer.USERNAME,
            SFTPTransfer.PASSWORD,
            SFTPTransfer.PRIVATE_KEY_PATH,
            SFTPTransfer.PRIVATE_KEY_PASSPHRASE,
            SFTPTransfer.STRICT_HOST_KEY_CHECKING,
            SFTPTransfer.HOST_KEY_FILE,
            SFTPTransfer.BATCH_SIZE,
            SFTPTransfer.CONNECTION_TIMEOUT,
            SFTPTransfer.DATA_TIMEOUT,
            SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT,
            SFTPTransfer.USE_COMPRESSION,
            SFTPTransfer.PROXY_CONFIGURATION_SERVICE,
            SFTPTransfer.CIPHERS_ALLOWED,
            SFTPTransfer.KEY_ALGORITHMS_ALLOWED,
            SFTPTransfer.KEY_EXCHANGE_ALGORITHMS_ALLOWED,
            SFTPTransfer.MESSAGE_AUTHENTICATION_CODES_ALLOWED
    );

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        FTPTransfer.migrateProxyProperties(config);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        String hostname = context.getProperty(FileTransfer.HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();

        final int maxNumberOfFiles = context.getProperty(SFTPTransfer.BATCH_SIZE).asInteger();
        int fileCount = 0;

        try (final SFTPTransfer transfer = new SFTPTransfer(context, logger)) {
            do {
                //evaluate again inside the loop as each flowfile can have a different hostname
                hostname = context.getProperty(FileTransfer.HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();

                final long startNanos = System.nanoTime();

                final String directoryPathProperty = context.getProperty(DIRECTORY_PATH).evaluateAttributeExpressions(flowFile).getValue();
                final String filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();

                try {
                    final Path directoryPath = Paths.get(directoryPathProperty).normalize();
                    final Path filePath = directoryPath.resolve(filename).normalize();

                    if (!directoryPath.equals(filePath.getParent())) {
                        final String errorMessage = "Attempting to delete file at path '%s' which is not a direct child of the directory '%s'"
                                .formatted(filePath, directoryPath);

                        handleFailure(session, flowFile, errorMessage, null);
                        continue;
                    }

                    transfer.deleteFile(flowFile, directoryPath.toString(), filename);

                    session.transfer(flowFile, REL_SUCCESS);
                    final String transitUri = "sftp://%s".formatted(filePath);
                    final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    logger.debug("Successfully deleted file at path {} in {} millis; routing to success", flowFile, transferMillis);
                    session.getProvenanceReporter().invokeRemoteProcess(flowFile, transitUri, "Object deleted");
                } catch (FileNotFoundException fileNotFoundException) {
                    session.transfer(flowFile, REL_NOT_FOUND);
                } catch (IOException ioException) {
                    final String errorMessage = "Failed to delete file '%s' in directory '%s'"
                            .formatted(filename, directoryPathProperty);

                    handleFailure(session, flowFile, errorMessage, ioException);
                }
            } while (isScheduled()
                    && (getRelationships().size() == context.getAvailableRelationships().size())
                    && (++fileCount < maxNumberOfFiles)
                    && ((flowFile = session.get()) != null));
        } catch (final IOException | FlowFileAccessException | ProcessException e) {
            context.yield();

            Throwable cause = switch (e) {
                case FlowFileAccessException ffaEx -> ffaEx.getCause();
                case ProcessException pEx -> pEx.getCause();
                default -> e;
            };

            String errorMessage = "Routing to failure since unable to delete %s from remote host %s".formatted(flowFile, hostname);
            handleFailure(session, flowFile, errorMessage, cause);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();
        SFTPTransfer.validateProxySpec(validationContext, results);
        return results;
    }

    private void handleFailure(ProcessSession session, FlowFile flowFile, String errorMessage, Throwable throwable) {
        getLogger().error(errorMessage, throwable);

        session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }
}