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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.smb.util.ServiceLocationFlowFileFilter;
import org.apache.nifi.services.smb.SmbClientProvider;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbShareAccess;
import org.apache.nifi.services.smb.SmbjClientProvider;
import org.apache.nifi.util.StringUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.smb.util.LocalSmbProperties.CONNECTION_CONFIGURATION_STRATEGY;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.ConnectionConfigurationStrategy;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.DOMAIN;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.ENABLE_DFS;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.HOSTNAME;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.PASSWORD;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.PORT;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.SHARE;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.SMB_CLIENT_PROVIDER_SERVICE;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.SMB_DIALECT;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.TIMEOUT;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.USERNAME;
import static org.apache.nifi.processors.smb.util.LocalSmbProperties.USE_ENCRYPTION;
import static org.apache.nifi.smb.common.SmbProperties.OLD_ENABLE_DFS_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_SMB_DIALECT_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_TIMEOUT_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_USE_ENCRYPTION_PROPERTY_NAME;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"samba, smb, cifs, files, put"})
@CapabilityDescription("Writes the contents of a FlowFile to an SMB network location (e.g. Samba or Windows Server). " +
    "Use this processor instead of a cifs mounts if share access control is important." +
    "Configure the Hostname, Share and Directory accordingly: \\\\[Hostname]\\[Share]\\[path\\to\\Directory]")
@SeeAlso({GetSmbFile.class, ListSmb.class, FetchSmb.class})
@ReadsAttributes({@ReadsAttribute(attribute = "filename", description = "The filename to use when writing the FlowFile to the network folder.")})
public class PutSmbFile extends AbstractProcessor {

    public static final char PATH_SEPARATOR = '/';

    public static final String SHARE_ACCESS_NONE = "none";
    public static final String SHARE_ACCESS_READ = "read";
    public static final String SHARE_ACCESS_READDELETE = "read, delete";
    public static final String SHARE_ACCESS_READWRITEDELETE = "read, write, delete";

    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The network folder to which files should be written. This is the remaining relative " +
            "path after the share: \\\\hostname\\share\\[dir1\\dir2]. You may use expression language.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CREATE_DIRS = new PropertyDescriptor.Builder()
            .name("Create Missing Directories")
            .description("If true, then missing destination directories will be created. If false, FlowFiles are penalized and sent to failure.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor SHARE_ACCESS = new PropertyDescriptor.Builder()
            .name("Share Access Strategy")
            .description("Indicates which shared access are granted on the file during the write. " +
                "None is the most restrictive, but the safest setting to prevent corruption.")
            .required(true)
            .defaultValue(SHARE_ACCESS_NONE)
            .allowableValues(SHARE_ACCESS_NONE, SHARE_ACCESS_READ, SHARE_ACCESS_READDELETE, SHARE_ACCESS_READWRITEDELETE)
            .build();
    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(REPLACE_RESOLUTION)
            .allowableValues(REPLACE_RESOLUTION, IGNORE_RESOLUTION, FAIL_RESOLUTION)
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of files to put in each iteration")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();
    public static final PropertyDescriptor RENAME_SUFFIX = new PropertyDescriptor.Builder()
            .name("Temporary Suffix")
            .description("A temporary suffix that is appended to the filename while it is being transferred. After the transfer is complete, the suffix will be removed.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to the output network path are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to the output network path for some reason are transferred to this relationship")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONNECTION_CONFIGURATION_STRATEGY,
            SMB_CLIENT_PROVIDER_SERVICE,
            HOSTNAME,
            PORT,
            SHARE,
            DIRECTORY,
            DOMAIN,
            USERNAME,
            PASSWORD,
            CREATE_DIRS,
            SHARE_ACCESS,
            CONFLICT_RESOLUTION,
            BATCH_SIZE,
            RENAME_SUFFIX,
            SMB_DIALECT,
            USE_ENCRYPTION,
            ENABLE_DFS,
            TIMEOUT);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private SmbClientProvider clientProvider;
    private Set<SmbShareAccess> sharedAccess;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientProvider = switch (context.getProperty(CONNECTION_CONFIGURATION_STRATEGY).asAllowableValue(ConnectionConfigurationStrategy.class)) {
            case CONTROLLER_SERVICE -> context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);
            case LOCAL_PROPERTIES -> new SmbjClientProvider(context, getLogger());
        };

        switch (context.getProperty(SHARE_ACCESS).getValue()) {
            case SHARE_ACCESS_NONE:
                sharedAccess = SmbShareAccess.NONE;
                break;
            case SHARE_ACCESS_READ:
                sharedAccess = SmbShareAccess.READ;
                break;
            case SHARE_ACCESS_READDELETE:
                sharedAccess = SmbShareAccess.READ_DELETE;
                break;
            case SHARE_ACCESS_READWRITEDELETE:
                sharedAccess = SmbShareAccess.READ_WRITE_DELETE;
                break;
        }
    }

    @OnStopped
    public void onStopped() {
        if (clientProvider instanceof SmbjClientProvider smbjClientProvider) {
            smbjClientProvider.close();
        }
        clientProvider = null;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.renameProperty(OLD_ENABLE_DFS_PROPERTY_NAME, ENABLE_DFS.getName());
        config.renameProperty(OLD_SMB_DIALECT_PROPERTY_NAME, SMB_DIALECT.getName());
        config.renameProperty(OLD_TIMEOUT_PROPERTY_NAME, TIMEOUT.getName());
        config.renameProperty(OLD_USE_ENCRYPTION_PROPERTY_NAME, USE_ENCRYPTION.getName());
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = new ArrayList<>();

        if (validationContext.getProperty(CONNECTION_CONFIGURATION_STRATEGY).asAllowableValue(ConnectionConfigurationStrategy.class) == ConnectionConfigurationStrategy.LOCAL_PROPERTIES) {
            if (validationContext.getProperty(USERNAME).isSet() && !validationContext.getProperty(PASSWORD).isSet()) {
                set.add(new ValidationResult.Builder().explanation("Password must be set if username is supplied.").build());
            }
        }

        return set;
    }

    String normalizePath(String path) {
        if (path == null) {
            return null;
        }

        return path.replace('\\', PATH_SEPARATOR)
                .replaceAll("/+", "/")
                .replaceAll("^/|/$", "");
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final ServiceLocationFlowFileFilter flowFileFilter = new ServiceLocationFlowFileFilter(clientProvider, batchSize);
        final List<FlowFile> flowFiles = session.get(flowFileFilter);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();
        logger.debug("Processing next {} FlowFiles", flowFiles.size());

        final URI serviceLocation = flowFileFilter.getSelectedServiceLocation();
        final Map<String, String> attributes = flowFileFilter.getSelectedAttributes();

        try (SmbClientService client = clientProvider.getClient(getLogger(), attributes)) {
            for (FlowFile flowFile : flowFiles) {
                try {
                    final long processingStartTime = System.nanoTime();

                    final String directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue();
                    final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());

                    final String destinationDirectory = normalizePath(directory);

                    final String destinationFullPath;

                    // build destination path for the flowfile
                    if (StringUtils.isBlank(destinationDirectory)) {
                        destinationFullPath = filename;
                    } else {
                        destinationFullPath = String.format("%s%c%s", destinationDirectory, PATH_SEPARATOR, filename);
                    }

                    // handle missing directory
                    final Boolean createMissingDirectories = context.getProperty(CREATE_DIRS).asBoolean();
                    if (StringUtils.isNotBlank(destinationDirectory) && !client.folderExists(destinationDirectory)) {
                        if (!createMissingDirectories) {
                            logger.warn("Penalizing {} and routing to failure as configured because the destination directory ({}) doesn't exist", flowFile, destinationDirectory);
                            flowFile = session.penalize(flowFile);
                            session.transfer(flowFile, REL_FAILURE);
                            continue;
                        } else {
                            try {
                                client.ensureDirectory(destinationDirectory);
                            } catch (Exception e) {
                                logger.error("Penalizing {} and routing to failure because failed to create missing destination directories ({})", flowFile, destinationDirectory, e);
                                flowFile = session.penalize(flowFile);
                                session.transfer(flowFile, REL_FAILURE);
                                continue;
                            }
                        }
                    }

                    // handle conflict resolution
                    final String conflictResolution = context.getProperty(CONFLICT_RESOLUTION).getValue();
                    if (client.fileExists(destinationFullPath)) {
                        if (conflictResolution.equals(IGNORE_RESOLUTION)) {
                            logger.info("Transferring {} to success as configured because file with same name already exists", flowFile);
                            session.transfer(flowFile, REL_SUCCESS);
                            continue;
                        } else if (conflictResolution.equals(FAIL_RESOLUTION)) {
                            logger.warn("Penalizing {} and routing to failure as configured because file with the same name already exists", flowFile);
                            flowFile = session.penalize(flowFile);
                            session.transfer(flowFile, REL_FAILURE);
                            continue;
                        }
                    }

                    // handle temporary suffix
                    final String renameSuffixValue = context.getProperty(RENAME_SUFFIX).getValue();
                    final boolean renameSuffix = StringUtils.isNotBlank(renameSuffixValue);
                    final String transferDestinationFullPath;
                    if (renameSuffix) {
                        transferDestinationFullPath = destinationFullPath + renameSuffixValue;
                    } else {
                        transferDestinationFullPath = destinationFullPath;
                    }

                    // handle the transfer
                    try {
                        session.read(flowFile, inputStream -> client.writeFile(transferDestinationFullPath, inputStream, sharedAccess));
                    } catch (Exception e) {
                        logger.error("Cannot transfer the file. Penalizing {} and routing to 'failure'", flowFile, e);
                        flowFile = session.penalize(flowFile);
                        session.transfer(flowFile, REL_FAILURE);
                        continue;
                    }

                    // handle the rename
                    if (renameSuffix) {
                        try {
                            client.renameFile(transferDestinationFullPath, destinationFullPath, true);
                        } catch (Exception e) {
                            logger.error("Cannot rename the file. Penalizing {} and routing to 'failure'", flowFile, e);
                            flowFile = session.penalize(flowFile);
                            session.transfer(flowFile, REL_FAILURE);
                            continue;
                        }
                    }

                    // handle the success
                    final String transitUri = String.format("%s/%s", serviceLocation, destinationFullPath.replace('\\', '/'));
                    final long processingTimeInNano = System.nanoTime() - processingStartTime;
                    final long processingTimeInMilli = TimeUnit.MILLISECONDS.convert(processingTimeInNano, TimeUnit.NANOSECONDS);
                    session.getProvenanceReporter().send(flowFile, transitUri, processingTimeInMilli);
                    session.transfer(flowFile, REL_SUCCESS);
                } catch (Exception e) {
                    logger.error("Error processing flowfile {}", flowFile, e);
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                }
            }
        } catch (Exception e) {
            logger.error("Could not establish smb connection", e);
            session.transfer(flowFiles, REL_FAILURE);
        }
    }
}
