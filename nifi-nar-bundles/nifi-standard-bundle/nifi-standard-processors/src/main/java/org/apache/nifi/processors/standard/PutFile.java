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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"put", "local", "copy", "archive", "files", "filesystem"})
@CapabilityDescription("Writes the contents of a FlowFile to the local file system. There are four strategies for filename conflict resolution to define the behavior "
        + "when a file currently exists in the output directory and risks being overwritten. The 'replace' strategy always writes FlowFile content, overwriting an existing file if necessary. "
        + "The 'ignore' strategy writes to the output directory unless there is an existing file with the same name; the FlowFile is always routed to success even when "
        + "content is not written to disk due to filename conflict. The 'fail' strategy writes to the output directory and routes the FlowFile to success unless there is a filename conflict. "
        + "When a conflict exists, content is not written to disk and the FlowFile is routed to failure. The 'archive' strategy moves an existing file to the archive directory when a conflict "
        + "exists, optionally applying an extension to the filename in the archive directory. If the filename already exists in the archive directory, content is not written to disk and the "
        + "FlowFile is routed to failure."
+ "")
@SeeAlso({FetchFile.class, GetFile.class})
@ReadsAttribute(attribute = "filename", description = "The filename to use when writing the FlowFile to disk.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
                        explanation = "Provides operator the ability to write to any file that NiFi has access to.")
        }
)
public class PutFile extends AbstractProcessor {

    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";
    public static final String ARCHIVE_RESOLUTION = "archive";
    public static final AllowableValue REPLACE = new AllowableValue(REPLACE_RESOLUTION, REPLACE_RESOLUTION,
            "FlowFile content is written to a file in the output directory overwriting the file if it already exists");
    public static final AllowableValue IGNORE = new AllowableValue(IGNORE_RESOLUTION, IGNORE_RESOLUTION,
            "If a file exists in the output directory with the same name as the filename attribute of the FlowFile, the file in the output directory remains unchanged "
                    + "and the FlowFile is routed to 'success'");
    public static final AllowableValue FAIL = new AllowableValue(FAIL_RESOLUTION, FAIL_RESOLUTION,
            "If a file exists in the output directory with the same name as the filename attribute of the FlowFile, the file in the output directory remains unchanged "
                    + "and the FlowFile is routed to 'failure'");
    public static final AllowableValue ARCHIVE = new AllowableValue(ARCHIVE_RESOLUTION, ARCHIVE_RESOLUTION,
            "If a file exists in the output directory with the same name as the filename attribute of the FlowFile, the existing file is moved to an archive location prior to "
                    + "writing FlowFile content to prevent the file from being overwritten");
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    public static final Pattern RWX_PATTERN = Pattern.compile("^([r-][w-])([x-])([r-][w-])([x-])([r-][w-])([x-])$");
    public static final Pattern NUM_PATTERN = Pattern.compile("^[0-7]{3}$");

    private static final Validator PERMISSIONS_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            ValidationResult.Builder vr = new ValidationResult.Builder();
            if (context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }

            if (RWX_PATTERN.matcher(input).matches() || NUM_PATTERN.matcher(input).matches()) {
                return vr.valid(true).build();
            }
            return vr.valid(false)
                    .subject(subject)
                    .input(input)
                    .explanation("This must be expressed in rwxr-x--- form or octal triplet form.")
                    .build();
        }
    };

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .displayName("Directory")
            .description("The directory to which files should be written. You may use expression language such as /aa/bb/${path}")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MAX_DESTINATION_FILES = new PropertyDescriptor.Builder()
            .name("Maximum File Count")
            .displayName("Maximum File Count")
            .description("Specifies the maximum number of files that can exist in the output directory")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .displayName("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory. "
                    + "If strategy is 'archive', then 'Archive Directory' property must be specified.")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(REPLACE, IGNORE, FAIL, ARCHIVE)
            .build();
    public static final PropertyDescriptor CHANGE_LAST_MODIFIED_TIME = new PropertyDescriptor.Builder()
            .name("Last Modified Time")
            .displayName("Last Modified Time")
            .description("Sets the lastModifiedTime on the output file to the value of this attribute.  Format must be yyyy-MM-dd'T'HH:mm:ssZ.  "
                    + "You may also use expression language such as ${file.lastModifiedTime}.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHANGE_PERMISSIONS = new PropertyDescriptor.Builder()
            .name("Permissions")
            .displayName("Permissions")
            .description("Sets the permissions on the output file to the value of this attribute.  Format must be either UNIX rwxrwxrwx with a - in "
                    + "place of denied permissions (e.g. rw-r--r--) or an octal number (e.g. 644).  You may also use expression language such as "
                    + "${file.permissions}.")
            .required(false)
            .addValidator(PERMISSIONS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHANGE_OWNER = new PropertyDescriptor.Builder()
            .name("Owner")
            .displayName("Owner")
            .description("Sets the owner on the output file to the value of this attribute.  You may also use expression language such as "
                    + "${file.owner}. Note on many operating systems Nifi must be running as a super-user to have the permissions to set the file owner.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHANGE_GROUP = new PropertyDescriptor.Builder()
            .name("Group")
            .displayName("Group")
            .description("Sets the group on the output file to the value of this attribute.  You may also use expression language such "
                    + "as ${file.group}.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CREATE_DIRS = new PropertyDescriptor.Builder()
            .name("Create Missing Directories")
            .displayName("Create Missing Directories")
            .description("If true, then missing destination or archive directories will be created. "
                    + "If false, missing destination or archive directories will cause the FlowFile to be penalized and sent to failure.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor ARCHIVE_DIRECTORY = new PropertyDescriptor.Builder()
            .name("Archive Directory")
            .displayName("Archive Directory")
            .description("Directory to place an existing file if the FlowFile would otherwise overwrite the file.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(CONFLICT_RESOLUTION, ARCHIVE_RESOLUTION)
            .build();
    public static final PropertyDescriptor ARCHIVE_FILENAME_EXTENSION = new PropertyDescriptor.Builder()
            .name("Archive Filename Extension")
            .displayName("Archive Filename Extension")
            .description("Extension added to the end of the filename when it is archived.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(CONFLICT_RESOLUTION, ARCHIVE_RESOLUTION)
            .build();
    public static final PropertyDescriptor MAX_ARCHIVE_FILES = new PropertyDescriptor.Builder()
            .name("Maximum Archive File Count")
            .displayName("Maximum Archive File Count")
            .description("Specifies the maximum number of archive files that can exist in the archive directory.")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .dependsOn(CONFLICT_RESOLUTION, ARCHIVE_RESOLUTION)
            .build();
    public static final PropertyDescriptor GUARANTEE_UNIQUE_ARCHIVE_FILENAME = new PropertyDescriptor.Builder()
            .name("Guarantee Archive Filename Uniqueness")
            .displayName("Guarantee Archive Filename Uniqueness")
            .description("If true and the archive filename already exists, a unique identifier will be added to the end of the new archive filename being created.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .dependsOn(CONFLICT_RESOLUTION, ARCHIVE_RESOLUTION)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to the output directory are transferred to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to the output directory for some reason are transferred to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> results = new ArrayList<>();

        if (validationContext.getProperty(CONFLICT_RESOLUTION).getValue().equals(ARCHIVE_RESOLUTION)) {
            if (!validationContext.getProperty(ARCHIVE_DIRECTORY).isSet()) {
                results.add(new ValidationResult.Builder()
                        .subject(ARCHIVE_DIRECTORY.getName())
                        .explanation("value must be provided when Conflict Resolution is set to 'archive'")
                        .valid(false)
                        .build());
            }
        }
        return results;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        // relationships
        final Set<Relationship> procRels = new HashSet<>();
        procRels.add(REL_SUCCESS);
        procRels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(procRels);

        // descriptors
        final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
        supDescriptors.add(DIRECTORY);
        supDescriptors.add(CONFLICT_RESOLUTION);
        supDescriptors.add(CREATE_DIRS);
        supDescriptors.add(MAX_DESTINATION_FILES);
        supDescriptors.add(CHANGE_LAST_MODIFIED_TIME);
        supDescriptors.add(CHANGE_PERMISSIONS);
        supDescriptors.add(CHANGE_OWNER);
        supDescriptors.add(CHANGE_GROUP);
        supDescriptors.add(ARCHIVE_DIRECTORY);
        supDescriptors.add(ARCHIVE_FILENAME_EXTENSION);
        supDescriptors.add(MAX_ARCHIVE_FILES);
        supDescriptors.add(GUARANTEE_UNIQUE_ARCHIVE_FILENAME);
        properties = Collections.unmodifiableList(supDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        final Path configuredRootDirPath = Paths.get(context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue());
        final String conflictResponse = context.getProperty(CONFLICT_RESOLUTION).getValue();
        final Integer maxDestinationFiles = context.getProperty(MAX_DESTINATION_FILES).asInteger();
        final Integer maxArchiveFiles = context.getProperty(MAX_ARCHIVE_FILES).asInteger();
        final ComponentLog logger = getLogger();
        final boolean archiveEnabled = context.getProperty(ARCHIVE_DIRECTORY).isSet();

        Path tempDotCopyFile = null;
        try {
            final Path rootDirPath = configuredRootDirPath.toAbsolutePath();
            String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final Path dotCopyFile = rootDirPath.resolve("." + filename); //tempCopyFile;
            tempDotCopyFile = dotCopyFile;
            Path finalCopyFile = rootDirPath.resolve(filename);

            String archiveFilename = "";
            Path archiveFile = Paths.get("");
            Path archiveDirPath = Paths.get("");
            if (archiveEnabled) {
                final Path configuredArchiveDirPath = Paths.get(context.getProperty(ARCHIVE_DIRECTORY).evaluateAttributeExpressions(flowFile).getValue());
                archiveDirPath = configuredArchiveDirPath.toAbsolutePath();
                archiveFilename = archiveDirPath + "/" + filename;
                String extension = context.getProperty(ARCHIVE_FILENAME_EXTENSION).evaluateAttributeExpressions(flowFile).getValue();
                if (extension != null)
                        archiveFilename += extension;
                archiveFile = Paths.get(archiveFilename);

                if (Files.exists(archiveFile) && context.getProperty(GUARANTEE_UNIQUE_ARCHIVE_FILENAME).asBoolean()) {
                    archiveFilename = String.join("-", archiveFilename, UUID.randomUUID().toString());
                    archiveFile = Paths.get(archiveFilename);
                }
            }

            final String permissions = context.getProperty(CHANGE_PERMISSIONS).evaluateAttributeExpressions(flowFile).getValue();
            final String owner = context.getProperty(CHANGE_OWNER).evaluateAttributeExpressions(flowFile).getValue();
            final String group = context.getProperty(CHANGE_GROUP).evaluateAttributeExpressions(flowFile).getValue();
            final boolean createDirectories = context.getProperty(CREATE_DIRS).asBoolean();
            boolean success = createDirectory(rootDirPath, createDirectories, permissions, owner, group, flowFile, logger);
            // Penalize FlowFile and send to failure if the target directory does not exist and cannot be created
            if (!success) {
                logger.warn("Penalizing {} and routing to 'failure' because output directory does not exist and could not be created: {}",
                        flowFile, archiveDirPath.toAbsolutePath());
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            final Path finalCopyFileDir = finalCopyFile.getParent();
            if (Files.exists(finalCopyFileDir) && maxDestinationFiles != null) { // check if too many files already
                final long numFiles = getFilesNumberInFolder(finalCopyFileDir, filename);

                if (numFiles >= maxDestinationFiles) {
                    flowFile = session.penalize(flowFile);
                    logger.warn("Penalizing {} and routing to 'failure' because the output directory {} has {} files, which equals or exceeds the "
                            + "configured maximum number of files", flowFile, finalCopyFileDir, numFiles );
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
            }

            if (archiveEnabled && Files.exists(archiveDirPath) && maxArchiveFiles != null) { // check if too many files in archive directory already
                final long  numFiles = getFilesNumberInFolder(archiveDirPath, archiveFilename);

                if (numFiles >= maxArchiveFiles) {
                    flowFile = session.penalize(flowFile);
                    logger.warn("Penalizing {} and routing to 'failure' because the archive directory {} has {} files, which equals or exceeds the "
                            + "configured maximum number of files", flowFile, archiveDirPath, numFiles);
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
            }

            if (Files.exists(finalCopyFile)) {
                switch (conflictResponse) {
                    case REPLACE_RESOLUTION:
                        Files.delete(finalCopyFile);
                        logger.info("Deleted {} as configured in order to replace with the contents of {}", finalCopyFile.toAbsolutePath(), flowFile);
                        break;
                    case IGNORE_RESOLUTION:
                        session.transfer(flowFile, REL_SUCCESS);
                        logger.info("Transferring {} to success and overwriting file '{}' because Conflict Resolution Strategy is set to 'ignore'", flowFile, finalCopyFile.toAbsolutePath());
                        return;
                    case FAIL_RESOLUTION:
                        flowFile = session.penalize(flowFile);
                        logger.warn("Penalizing {} and routing to 'failure' as configured because file with the same name already exists", flowFile);
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    case ARCHIVE_RESOLUTION:
                        // attempt to create archive directory
                        success = createDirectory(archiveDirPath, createDirectories, permissions, owner, group, flowFile, logger);
                        // Penalize FlowFile and send to failure if the archive directory does not exist and cannot be created
                        if (!success) {
                            logger.warn("Penalizing {} and routing to 'failure' because archive directory does not exist and could not be created: {}", flowFile, archiveDirPath.toAbsolutePath());
                            flowFile = session.penalize(flowFile);
                            session.transfer(flowFile, REL_FAILURE);
                            return;
                        }
                        // If archive file already exists, follow fail resolution strategy: penalize and route to failure
                        if (Files.exists(archiveFile)) {
                            flowFile = session.penalize(flowFile);
                            logger.warn("Penalizing {} and routing to 'failure' because archive file with the same name already exists: {}",
                                    flowFile, archiveFile.toAbsolutePath());
                            session.transfer(flowFile, REL_FAILURE);
                            return;
                        }
                        // Archive existing file before writing current FlowFile to prevent overwriting the file
                        logger.info("Archiving file {} to {} before replacing with FlowFile {}", finalCopyFile.toAbsolutePath(), archiveFile.toAbsolutePath(), flowFile);
                        Files.move(finalCopyFile, archiveFile);
                    default:
                        break;
                }
            }

            session.exportTo(flowFile, dotCopyFile, false);

            final String lastModifiedTime = context.getProperty(CHANGE_LAST_MODIFIED_TIME).evaluateAttributeExpressions(flowFile).getValue();
            if (lastModifiedTime != null && !lastModifiedTime.trim().isEmpty()) {
                try {
                    final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                    final Date fileModifyTime = formatter.parse(lastModifiedTime);
                    dotCopyFile.toFile().setLastModified(fileModifyTime.getTime());
                } catch (Exception e) {
                    logger.warn("Could not set file lastModifiedTime to {} because {}", lastModifiedTime, e);
                }
            }

            if (permissions != null && !permissions.trim().isEmpty()) {
                try {
                    String perms = stringPermissions(permissions, false);
                    if (!perms.isEmpty()) {
                        Files.setPosixFilePermissions(dotCopyFile, PosixFilePermissions.fromString(perms));
                    }
                } catch (Exception e) {
                    logger.warn("Could not set file permissions to {} because {}", permissions, e);
                }
            }

            if (owner != null && !owner.trim().isEmpty()) {
                try {
                    UserPrincipalLookupService lookupService = dotCopyFile.getFileSystem().getUserPrincipalLookupService();
                    Files.setOwner(dotCopyFile, lookupService.lookupPrincipalByName(owner));
                } catch (Exception e) {
                    logger.warn("Could not set file owner to {} because {}", owner, e);
                }
            }

            if (group != null && !group.trim().isEmpty()) {
                try {
                    UserPrincipalLookupService lookupService = dotCopyFile.getFileSystem().getUserPrincipalLookupService();
                    PosixFileAttributeView view = Files.getFileAttributeView(dotCopyFile, PosixFileAttributeView.class);
                    view.setGroup(lookupService.lookupPrincipalByGroupName(group));
                } catch (Exception e) {
                    logger.warn("Could not set file group to {} because {}", group, e);
                }
            }

            boolean renamed = false;
            for (int i = 0; i < 10; i++) { // try rename up to 10 times.
                if (dotCopyFile.toFile().renameTo(finalCopyFile.toFile())) {
                    renamed = true;
                    break;// rename was successful
                }
                Thread.sleep(100L);// try waiting a few ms to let whatever might cause rename failure to resolve
            }

            if (!renamed) {
                if (Files.exists(dotCopyFile) && dotCopyFile.toFile().delete()) {
                    logger.debug("Deleted dot copy file {}", dotCopyFile);
                }
                throw new ProcessException("Could not rename: " + dotCopyFile);
            } else {
                logger.info("Produced copy of {} at location {}", flowFile, finalCopyFile);
            }

            session.getProvenanceReporter().send(flowFile, finalCopyFile.toFile().toURI().toString(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Throwable t) {
            if (tempDotCopyFile != null) {
                try {
                    Files.deleteIfExists(tempDotCopyFile);
                } catch (final Exception e) {
                    logger.error("Unable to remove temporary file {} due to {}", tempDotCopyFile, e);
                }
            }

            flowFile = session.penalize(flowFile);
            logger.error("Penalizing {} and transferring to failure due to {}", flowFile, t);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private long getFilesNumberInFolder(Path folder, String filenameToIgnore) {
        String[] filesInFolder = folder.toFile().list();
        return filesInFolder == null ? 0 : Arrays.stream(filesInFolder)
                .filter(eachFilename -> !eachFilename.equals(filenameToIgnore))
                .count();
    }

    private boolean createDirectory(final Path rootDirPath, final boolean createDirs, final String permissions, final String owner, final String group,
                                    final FlowFile flowFile, final ComponentLog logger) throws IOException {
        if (!Files.exists(rootDirPath)) {
            if (createDirs) {
                Path existing = rootDirPath;
                while (!Files.exists(existing)) {
                    existing = existing.getParent();
                }
                if (permissions != null && !permissions.trim().isEmpty()) {
                    try {
                        String perms = stringPermissions(permissions, true);
                        if (!perms.isEmpty()) {
                            Files.createDirectories(rootDirPath, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(perms)));
                        } else {
                            Files.createDirectories(rootDirPath);
                        }
                    } catch (Exception e) {
                        logger.error("Could not set create directory with permissions {} because {}", permissions, e);
                        return false;
                    }
                } else {
                    Files.createDirectories(rootDirPath);
                }

                boolean chOwner = owner != null && !owner.trim().isEmpty();
                boolean chGroup = group != null && !group.trim().isEmpty();
                if (chOwner || chGroup) {
                    Path currentPath = rootDirPath;
                    while (!currentPath.equals(existing)) {
                        if (chOwner) {
                            try {
                                UserPrincipalLookupService lookupService = currentPath.getFileSystem().getUserPrincipalLookupService();
                                Files.setOwner(currentPath, lookupService.lookupPrincipalByName(owner));
                            } catch (Exception e) {
                                logger.warn("Could not set directory owner to {} because {}", owner, e);
                            }
                        }
                        if (chGroup) {
                            try {
                                UserPrincipalLookupService lookupService = currentPath.getFileSystem().getUserPrincipalLookupService();
                                PosixFileAttributeView view = Files.getFileAttributeView(currentPath, PosixFileAttributeView.class);
                                view.setGroup(lookupService.lookupPrincipalByGroupName(group));
                            } catch (Exception e) {
                                logger.warn("Could not set file group to {} because {}", group, e);
                            }
                        }
                        currentPath = currentPath.getParent();
                    }
                }
            } else {
                return false;
            }
        }
        return true;
    }

    protected String stringPermissions(String perms, boolean directory) {
        String permissions = "";
        Matcher rwx = RWX_PATTERN.matcher(perms);
        if (rwx.matches()) {
            if (directory) {
                // To read or write, directory access will be required
                StringBuilder permBuilder = new StringBuilder();
                permBuilder.append("$1");
                permBuilder.append(rwx.group(1).equals("--") ? "$2" : "x");
                permBuilder.append("$3");
                permBuilder.append(rwx.group(3).equals("--") ? "$4" : "x");
                permBuilder.append("$5");
                permBuilder.append(rwx.group(5).equals("--") ? "$6" : "x");
                permissions = rwx.replaceAll(permBuilder.toString());
            } else {
                permissions = perms;
            }
        } else if (NUM_PATTERN.matcher(perms).matches()) {
            try {
                int number = Integer.parseInt(perms, 8);
                StringBuilder permBuilder = new StringBuilder();
                if ((number & 0x100) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x80) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if (directory || (number & 0x40) > 0) {
                    permBuilder.append('x');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x20) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x10) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x8) > 0) {
                    permBuilder.append('x');
                } else {
                    if (directory && (number & 0x30) > 0) {
                        // To read or write, directory access will be required
                        permBuilder.append('x');
                    } else {
                        permBuilder.append('-');
                    }
                }
                if ((number & 0x4) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x2) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x1) > 0) {
                    permBuilder.append('x');
                } else {
                    if (directory && (number & 0x6) > 0) {
                        // To read or write, directory access will be required
                        permBuilder.append('x');
                    } else {
                        permBuilder.append('-');
                    }
                }
                permissions = permBuilder.toString();
            } catch (NumberFormatException ignore) {
            }
        }

        return permissions;
    }
}
