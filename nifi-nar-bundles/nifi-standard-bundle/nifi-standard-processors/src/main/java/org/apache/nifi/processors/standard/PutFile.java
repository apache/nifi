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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"put", "local", "copy", "archive", "files", "filesystem"})
@CapabilityDescription("Writes the contents of a FlowFile to the local file system")
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

    public static final String FILE_MODIFY_DATE_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The directory to which files should be written. You may use expression language such as /aa/bb/${path}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MAX_DESTINATION_FILES = new PropertyDescriptor.Builder()
            .name("Maximum File Count")
            .description("Specifies the maximum number of files that can exist in the output directory")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(REPLACE_RESOLUTION, IGNORE_RESOLUTION, FAIL_RESOLUTION)
            .build();
    public static final PropertyDescriptor CHANGE_LAST_MODIFIED_TIME = new PropertyDescriptor.Builder()
            .name("Last Modified Time")
            .description("Sets the lastModifiedTime on the output file to the value of this attribute.  Format must be yyyy-MM-dd'T'HH:mm:ssZ.  "
                    + "You may also use expression language such as ${file.lastModifiedTime}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHANGE_PERMISSIONS = new PropertyDescriptor.Builder()
            .name("Permissions")
            .description("Sets the permissions on the output file to the value of this attribute.  Format must be either UNIX rwxrwxrwx with a - in "
                    + "place of denied permissions (e.g. rw-r--r--) or an octal number (e.g. 644).  You may also use expression language such as "
                    + "${file.permissions}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHANGE_OWNER = new PropertyDescriptor.Builder()
            .name("Owner")
            .description("Sets the owner on the output file to the value of this attribute.  You may also use expression language such as "
                    + "${file.owner}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHANGE_GROUP = new PropertyDescriptor.Builder()
            .name("Group")
            .description("Sets the group on the output file to the value of this attribute.  You may also use expression language such "
                    + "as ${file.group}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CREATE_DIRS = new PropertyDescriptor.Builder()
            .name("Create Missing Directories")
            .description("If true, then missing destination directories will be created. If false, flowfiles are penalized and sent to failure.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final int MAX_FILE_LOCK_ATTEMPTS = 10;
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
        final ComponentLog logger = getLogger();

        Path tempDotCopyFile = null;
        try {
            final Path rootDirPath = configuredRootDirPath;
            final Path tempCopyFile = rootDirPath.resolve("." + flowFile.getAttribute(CoreAttributes.FILENAME.key()));
            final Path copyFile = rootDirPath.resolve(flowFile.getAttribute(CoreAttributes.FILENAME.key()));

            if (!Files.exists(rootDirPath)) {
                if (context.getProperty(CREATE_DIRS).asBoolean()) {
                    Files.createDirectories(rootDirPath);
                } else {
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                    logger.error("Penalizing {} and routing to 'failure' because the output directory {} does not exist and Processor is "
                            + "configured not to create missing directories", new Object[]{flowFile, rootDirPath});
                    return;
                }
            }

            final Path dotCopyFile = tempCopyFile;
            tempDotCopyFile = dotCopyFile;
            Path finalCopyFile = copyFile;

            final Path finalCopyFileDir = finalCopyFile.getParent();
            if (Files.exists(finalCopyFileDir) && maxDestinationFiles != null) { // check if too many files already
                final int numFiles = finalCopyFileDir.toFile().list().length;

                if (numFiles >= maxDestinationFiles) {
                    flowFile = session.penalize(flowFile);
                    logger.warn("Penalizing {} and routing to 'failure' because the output directory {} has {} files, which exceeds the "
                            + "configured maximum number of files", new Object[]{flowFile, finalCopyFileDir, numFiles});
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
            }

            if (Files.exists(finalCopyFile)) {
                switch (conflictResponse) {
                    case REPLACE_RESOLUTION:
                        Files.delete(finalCopyFile);
                        logger.info("Deleted {} as configured in order to replace with the contents of {}", new Object[]{finalCopyFile, flowFile});
                        break;
                    case IGNORE_RESOLUTION:
                        session.transfer(flowFile, REL_SUCCESS);
                        logger.info("Transferring {} to success because file with same name already exists", new Object[]{flowFile});
                        return;
                    case FAIL_RESOLUTION:
                        flowFile = session.penalize(flowFile);
                        logger.warn("Penalizing {} and routing to failure as configured because file with the same name already exists", new Object[]{flowFile});
                        session.transfer(flowFile, REL_FAILURE);
                        return;
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
                    logger.warn("Could not set file lastModifiedTime to {} because {}", new Object[]{lastModifiedTime, e});
                }
            }

            final String permissions = context.getProperty(CHANGE_PERMISSIONS).evaluateAttributeExpressions(flowFile).getValue();
            if (permissions != null && !permissions.trim().isEmpty()) {
                try {
                    String perms = stringPermissions(permissions);
                    if (!perms.isEmpty()) {
                        Files.setPosixFilePermissions(dotCopyFile, PosixFilePermissions.fromString(perms));
                    }
                } catch (Exception e) {
                    logger.warn("Could not set file permissions to {} because {}", new Object[]{permissions, e});
                }
            }

            final String owner = context.getProperty(CHANGE_OWNER).evaluateAttributeExpressions(flowFile).getValue();
            if (owner != null && !owner.trim().isEmpty()) {
                try {
                    UserPrincipalLookupService lookupService = dotCopyFile.getFileSystem().getUserPrincipalLookupService();
                    Files.setOwner(dotCopyFile, lookupService.lookupPrincipalByName(owner));
                } catch (Exception e) {
                    logger.warn("Could not set file owner to {} because {}", new Object[]{owner, e});
                }
            }

            final String group = context.getProperty(CHANGE_GROUP).evaluateAttributeExpressions(flowFile).getValue();
            if (group != null && !group.trim().isEmpty()) {
                try {
                    UserPrincipalLookupService lookupService = dotCopyFile.getFileSystem().getUserPrincipalLookupService();
                    PosixFileAttributeView view = Files.getFileAttributeView(dotCopyFile, PosixFileAttributeView.class);
                    view.setGroup(lookupService.lookupPrincipalByGroupName(group));
                } catch (Exception e) {
                    logger.warn("Could not set file group to {} because {}", new Object[]{group, e});
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
                    logger.debug("Deleted dot copy file {}", new Object[]{dotCopyFile});
                }
                throw new ProcessException("Could not rename: " + dotCopyFile);
            } else {
                logger.info("Produced copy of {} at location {}", new Object[]{flowFile, finalCopyFile});
            }

            session.getProvenanceReporter().send(flowFile, finalCopyFile.toFile().toURI().toString(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Throwable t) {
            if (tempDotCopyFile != null) {
                try {
                    Files.deleteIfExists(tempDotCopyFile);
                } catch (final Exception e) {
                    logger.error("Unable to remove temporary file {} due to {}", new Object[]{tempDotCopyFile, e});
                }
            }

            flowFile = session.penalize(flowFile);
            logger.error("Penalizing {} and transferring to failure due to {}", new Object[]{flowFile, t});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    protected String stringPermissions(String perms) {
        String permissions = "";
        final Pattern rwxPattern = Pattern.compile("^[rwx-]{9}$");
        final Pattern numPattern = Pattern.compile("\\d+");
        if (rwxPattern.matcher(perms).matches()) {
            permissions = perms;
        } else if (numPattern.matcher(perms).matches()) {
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
                if ((number & 0x40) > 0) {
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
                    permBuilder.append('-');
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
                if ((number & 0x8) > 0) {
                    permBuilder.append('x');
                } else {
                    permBuilder.append('-');
                }
                permissions = permBuilder.toString();
            } catch (NumberFormatException ignore) {
            }
        }
        return permissions;
    }
}
