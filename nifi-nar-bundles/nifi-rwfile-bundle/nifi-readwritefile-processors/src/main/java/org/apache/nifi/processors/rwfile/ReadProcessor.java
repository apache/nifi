package org.apache.nifi.processors.rwfile;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.rwfile.ReadFileService;
import org.oiue.tools.string.StringUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;

/**
 * @author every
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"local", "files", "filesystem", "ingest", "ingress", "get", "source", "input", "fetch"})
@CapabilityDescription("Reads the contents of a file from disk and streams it into the contents of an incoming FlowFile. Once this is done, the file is optionally moved " +
        "elsewhere or deleted to help keep the file system organized.")
@ReadsAttribute(attribute = "filename", description = "The filename to use when writing the FlowFile to disk.")
@Restricted(restrictions = {@Restriction(requiredPermission = RequiredPermission.READ_FILESYSTEM,
        explanation = "Provides operator the ability to read from any file that NiFi has access to."),
        @Restriction(requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
                explanation = "Provides operator the ability to delete any file that NiFi has access to.")})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class ReadProcessor extends AbstractProcessor {
    public static final AllowableValue COMPLETION_NONE = new AllowableValue("None", "None", "Leave the file as-is");
    public static final AllowableValue COMPLETION_MOVE = new AllowableValue("Move File", "Move File",
            "Moves the file to the directory specified by the <Move Destination Directory> property");
    public static final AllowableValue COMPLETION_DELETE = new AllowableValue("Delete File", "Delete File",
            "Deletes the original file from the file system");
    public static final AllowableValue CONFLICT_REPLACE = new AllowableValue("Replace File", "Replace File",
            "The newly ingested file should replace the existing file in the Destination Directory");
    public static final AllowableValue CONFLICT_KEEP_INTACT = new AllowableValue("Keep Existing", "Keep Existing",
            "The existing file should in the Destination Directory should stay intact and the newly ingested file should be deleted");
    public static final AllowableValue CONFLICT_FAIL = new AllowableValue("Fail", "Fail",
            "The existing destination file should remain intact and the incoming FlowFile should be routed to failure");
    public static final AllowableValue CONFLICT_RENAME = new AllowableValue("Rename", "Rename",
            "The existing destination file should remain intact. The newly ingested file should be moved to the destination directory but be renamed to a random filename");

    public static final PropertyDescriptor FILEPATH = (new PropertyDescriptor.Builder())
            .name("File to Fetch")
            .description("The fully-qualified filename of the file to fetch from the file system")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${absolute.path}/${filename}")
            .required(true)
            .build();
    public static final PropertyDescriptor CHARSET = (new PropertyDescriptor.Builder())
            .name("character-set")
            .displayName("Character Set")
            .description("The Character Encoding that is used to encode/decode the file")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .required(true)
            .build();
    public static final PropertyDescriptor IGNORE_HEADER = (new PropertyDescriptor.Builder())
            .name("ignore-header")
            .displayName("Ignore Header Rows")
            .description("If the first N lines are headers, will be ignored.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor NROWS = (new PropertyDescriptor.Builder())
            .name("rows")
            .displayName("N of lines read)")
            .description("N rows read.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor READFILE_SERVICE = new PropertyDescriptor
            .Builder().name("READFILE_SERVICE")
            .displayName("READFILE SERVICE")
            .description("Read file Property")
            .required(true)
            .identifiesControllerService(ReadFileService.class)
            .build();

    public static final PropertyDescriptor DELAY = (new PropertyDescriptor.Builder())
            .name("delay")
            .displayName("N of lines delay(milliseconds)")
            .description("Pause time per N rows read.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor COMPLETION_STRATEGY = (new PropertyDescriptor.Builder())
            .name("Completion Strategy")
            .description("Specifies what to do with the original file on the file system once it has been pulled into NiFi")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(COMPLETION_NONE, COMPLETION_MOVE, COMPLETION_DELETE)
            .defaultValue(COMPLETION_NONE.getValue())
            .required(true)
            .build();
    public static final PropertyDescriptor MOVE_DESTINATION_DIR = (new PropertyDescriptor.Builder())
            .name("Move Destination Directory")
            .description("The directory to the move the original file to once it has been fetched from the file system. This property is ignored unless the Completion Strategy " +
                    "is set to \"Move File\". If the directory does not exist, it will be created.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor CONFLICT_STRATEGY = (new PropertyDescriptor.Builder())
            .name("Move Conflict Strategy")
            .description("If Completion Strategy is set to Move File and a file already exists in the destination directory with the same name, this property specifies how that " +
                    "naming conflict should be resolved")
            .allowableValues(CONFLICT_RENAME, CONFLICT_REPLACE, CONFLICT_KEEP_INTACT, CONFLICT_FAIL)
            .defaultValue(CONFLICT_RENAME.getValue())
            .required(true)
            .build();
    public static final PropertyDescriptor FILE_NOT_FOUND_LOG_LEVEL = (new PropertyDescriptor.Builder())
            .name("Log level when file not found")
            .description("Log level to use in case the file does not exist when the processor is triggered")
            .allowableValues(LogLevel.values())
            .defaultValue(LogLevel.ERROR.toString())
            .required(true)
            .build();
    public static final PropertyDescriptor PERM_DENIED_LOG_LEVEL = (new PropertyDescriptor.Builder())
            .name("Log level when permission denied")
            .description("Log level to use in case user " + System.getProperty("user.name") + " does not have sufficient permissions to read the file")
            .allowableValues(LogLevel.values())
            .defaultValue(LogLevel.ERROR.toString())
            .required(true)
            .build();

    public static final Relationship REL_ORIGINAL = (new Relationship.Builder())
            .name("original")
            .description("The original file")
            .build();
    public static final Relationship REL_FRAGMENT = (new Relationship.Builder())
            .name("File fragment")
            .description("File fragment flowFile that is success read N lines from the file system will be transferred to this Relationship.")
            .build();
    public static final Relationship REL_HAS_NEXT = (new Relationship.Builder())
            .name("File has next fragment")
            .description("There is the next fragment of the file will be transferred to this Relationship.")
            .build();
    public static final Relationship REL_SUCCESS = (new Relationship.Builder())
            .name("success")
            .description("Any FlowFile that is successfully read from the file system will be transferred to this Relationship.")
            .build();
    public static final Relationship REL_NOT_FOUND = (new Relationship.Builder())
            .name("not.found")
            .description("Any FlowFile that could not be fetched from the file system because the file could not be found will be transferred to this Relationship.")
            .build();
    public static final Relationship REL_PERMISSION_DENIED = (new Relationship.Builder())
            .name("permission.denied")
            .description("Any FlowFile that could not be fetched from the file system due to the user running NiFi not having sufficient permissions will be transferred to this Relationship.")
            .build();
    public static final Relationship REL_FAILURE = (new Relationship.Builder())
            .name("failure")
            .description("Any FlowFile that could not be fetched from the file system for any reason other than insufficient permissions or the file not existing will be transferred to this Relationship.")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(FILEPATH);
        properties.add(CHARSET);
        properties.add(IGNORE_HEADER);
        properties.add(NROWS);
        properties.add(DELAY);
        properties.add(READFILE_SERVICE);
        properties.add(COMPLETION_STRATEGY);
        properties.add(MOVE_DESTINATION_DIR);
        properties.add(CONFLICT_STRATEGY);
        properties.add(FILE_NOT_FOUND_LOG_LEVEL);
        properties.add(PERM_DENIED_LOG_LEVEL);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FRAGMENT);
        relationships.add(REL_HAS_NEXT);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_NOT_FOUND);
        relationships.add(REL_PERMISSION_DENIED);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>();
        if (COMPLETION_MOVE.getValue()
                .equalsIgnoreCase(validationContext.getProperty(COMPLETION_STRATEGY).getValue()) && !validationContext.getProperty(MOVE_DESTINATION_DIR).isSet()) {
            results.add((new ValidationResult.Builder())
                    .subject(MOVE_DESTINATION_DIR.getName())
                    .input(null)
                    .valid(false)
                    .explanation(MOVE_DESTINATION_DIR.getName() + " must be specified if " + COMPLETION_STRATEGY.getName() + " is set to " + COMPLETION_MOVE.getDisplayName())
                    .build());
        }
        return results;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            Map<String,Object> readData;
            String completionStrategy = context.getProperty(COMPLETION_STRATEGY).getValue();
            String targetDirectoryName = context.getProperty(MOVE_DESTINATION_DIR).evaluateAttributeExpressions(flowFile).getValue();
            ReadFileService readFileService = context.getProperty(READFILE_SERVICE).asControllerService(ReadFileService.class);

            Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());
            String filepath = context.getProperty(FILEPATH).evaluateAttributeExpressions(flowFile).getValue();
            String fragmentId = attributes.get(FragmentAttributes.FRAGMENT_ID.key());
            boolean isFragment = !StringUtil.isEmptys(fragmentId);
            if (isFragment) {
                readData = readFileService.getFragment(filepath);
                if(readData == null){
                    int nRows = context.getProperty(NROWS).asInteger();
                    String charSet = context.getProperty(CHARSET).getValue();
                    int ignoreHeader = Integer.valueOf(attributes.get("read_count"));
                    readData =  readFileService.readFile(filepath, ignoreHeader, charSet, nRows);
                }
            }else{
                LogLevel levelFileNotFound = LogLevel.valueOf(context.getProperty(FILE_NOT_FOUND_LOG_LEVEL).getValue());
                LogLevel levelPermDenied = LogLevel.valueOf(context.getProperty(PERM_DENIED_LOG_LEVEL).getValue());

                int nRows = context.getProperty(NROWS).asInteger();
                int ignoreHeader = context.getProperty(IGNORE_HEADER).asInteger();
                String charSet = context.getProperty(CHARSET).getValue();

                File file = new File(filepath);
                // Verify that file system is reachable and file exists
                Path filePath = file.toPath();
                // see https://docs.oracle.com/javase/tutorial/essential/io/check.html for more details
                if (!Files.exists(filePath) && !Files.notExists(filePath)){
                    getLogger().log(levelFileNotFound, "Could not fetch file {} from file system for {} because the existence of the file cannot be verified; routing to failure",
                            new Object[] {file, flowFile});
                    session.transfer(session.penalize(flowFile), REL_FAILURE);
                    return;
                } else if (!Files.exists(filePath)) {
                    getLogger().log(levelFileNotFound, "Could not fetch file {} from file system for {} because the file does not exist; routing to not.found", new Object[] {file, flowFile});
                    session.getProvenanceReporter().route(flowFile, REL_NOT_FOUND);
                    session.transfer(session.penalize(flowFile), REL_NOT_FOUND);
                    return;
                }

                // Verify read permission on file
                final String user = System.getProperty("user.name");
                if (!isReadable(file)) {
                    getLogger().log(levelPermDenied, "Could not fetch file {} from file system for {} due to user {} not having sufficient permissions to read the file; routing to permission.denied",
                            new Object[] {file, flowFile, user});
                    session.getProvenanceReporter().route(flowFile, REL_PERMISSION_DENIED);
                    session.transfer(session.penalize(flowFile), REL_PERMISSION_DENIED);
                    return;
                }
                if (targetDirectoryName != null) {
                    final File targetDir = new File(targetDirectoryName);
                    if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
                        if (targetDir.exists() && (!isWritable(targetDir) || !isDirectory(targetDir))) {
                            getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, "
                                            + "but that is not a directory or user {} does not have permissions to write to that directory",
                                    new Object[] {file, flowFile, targetDir, user});
                            session.transfer(flowFile, REL_FAILURE);
                            return;
                        }

                        if (!targetDir.exists()) {
                            try {
                                Files.createDirectories(targetDir.toPath());
                            } catch (Exception e) {
                                getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, "
                                                + "but that directory does not exist and could not be created due to: {}",
                                        new Object[] {file, flowFile, targetDir, e.getMessage()}, e);
                                session.transfer(flowFile, REL_FAILURE);
                                return;
                            }
                        }

                        final String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).getValue();

                        if (CONFLICT_FAIL.getValue().equalsIgnoreCase(conflictStrategy)) {
                            final File targetFile = new File(targetDir, file.getName());
                            if (targetFile.exists()) {
                                getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, "
                                                + "but a file with name {} already exists in that directory and the Move Conflict Strategy is configured for failure",
                                        new Object[] {file, flowFile, targetDir, file.getName()});
                                session.transfer(flowFile, REL_FAILURE);
                                return;
                            }
                        }
                    }
                }
                fragmentId = attributes.get("uuid");
                readData =  readFileService.readFile(filepath, ignoreHeader, charSet, nRows);
                Map<String,String> fragmentInfo = new HashMap<>();
                fragmentInfo.put(FragmentAttributes.FRAGMENT_ID.key(),fragmentId);
                session.transfer(session.putAllAttributes(flowFile,fragmentInfo), REL_ORIGINAL);
            }

            getLogger().debug("read data:{}",new Object[]{readData});

            long i = (long) readData.get("fragment_index");
            attributes.put("read_count", readData.get("read_count")+"");
            attributes.put(FragmentAttributes.FRAGMENT_ID.key(), fragmentId);
            attributes.put(FragmentAttributes.FRAGMENT_INDEX.key(), Long.toString(i));

            FlowFile packet = session.create(flowFile);
            Map<String, Object> finalReadData = readData;
            packet = session.write(packet, out -> out.write(finalReadData.get("data").toString().getBytes()));
            session.transfer(session.putAllAttributes(packet, attributes), REL_FRAGMENT);

            if (!(boolean)readData.get("over")) {
                FlowFile has_packet = isFragment?flowFile:session.create(flowFile);
                session.transfer(session.putAllAttributes(has_packet, attributes), REL_HAS_NEXT);
                return;
            }

            attributes.remove(FragmentAttributes.FRAGMENT_INDEX.key());
            session.transfer(session.putAllAttributes(isFragment?flowFile:session.create(flowFile), attributes), REL_SUCCESS);

            // Attempt to perform the Completion Strategy action
            Exception completionFailureException = null;
            File file = new File(filepath);
            if (COMPLETION_DELETE.getValue().equalsIgnoreCase(completionStrategy)) {
                // convert to path and use Files.delete instead of file.delete so that if we fail, we know why
                try {
                    delete(file);
                } catch (final IOException ioe) {
                    completionFailureException = ioe;
                }
            } else if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
                final File targetDirectory = new File(targetDirectoryName);
                final File targetFile = new File(targetDirectory, file.getName());
                try {
                    if (targetFile.exists()) {
                        final String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).getValue();
                        if (CONFLICT_KEEP_INTACT.getValue().equalsIgnoreCase(conflictStrategy)) {
                            // don't move, just delete the original
                            Files.delete(file.toPath());
                        } else if (CONFLICT_RENAME.getValue().equalsIgnoreCase(conflictStrategy)) {
                            // rename to add a random UUID but keep the file extension if it has one.
                            final String simpleFilename = targetFile.getName();
                            final String newName;
                            if (simpleFilename.contains(".")) {
                                newName = StringUtils.substringBeforeLast(simpleFilename, ".") + "-" + UUID.randomUUID().toString() + "." + StringUtils.substringAfterLast(simpleFilename, ".");
                            } else {
                                newName = simpleFilename + "-" + UUID.randomUUID().toString();
                            }

                            move(file, new File(targetDirectory, newName), false);
                        } else if (CONFLICT_REPLACE.getValue().equalsIgnoreCase(conflictStrategy)) {
                            move(file, targetFile, true);
                        }
                    } else {
                        move(file, targetFile, false);
                    }
                } catch (final IOException ioe) {
                    completionFailureException = ioe;
                }
            }

            // Handle completion failures
            if (completionFailureException != null) {
                getLogger().warn("Successfully fetched the content from {} for {} but failed to perform Completion Action due to {}; routing to success",
                        new Object[] {file, flowFile, completionFailureException}, completionFailureException);
            }
        }catch (Throwable e){
            getLogger().error("",e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }


    //
    // The following set of methods exist purely for testing purposes
    //
    protected void move(final File source, final File target, final boolean overwrite) throws IOException {
        final File targetDirectory = target.getParentFile();

        // convert to path and use Files.move instead of file.renameTo so that if we fail, we know why
        final Path targetPath = target.toPath();
        if (!targetDirectory.exists()) {
            Files.createDirectories(targetDirectory.toPath());
        }

        final CopyOption[] copyOptions = overwrite ? new CopyOption[] {StandardCopyOption.REPLACE_EXISTING} : new CopyOption[] {};
        Files.move(source.toPath(), targetPath, copyOptions);
    }

    protected void delete(final File file) throws IOException {
        Files.delete(file.toPath());
    }

    protected boolean isReadable(final File file) {
        return file.canRead();
    }

    protected boolean isWritable(final File file) {
        return file.canWrite();
    }

    protected boolean isDirectory(final File file) {
        return file.isDirectory();
    }
}
