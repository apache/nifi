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
package org.apache.nifi.processors.standard.util;

import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processor.util.file.transfer.PermissionDeniedException;
import org.apache.nifi.processors.standard.ssh.SshClientProvider;
import org.apache.nifi.processors.standard.ssh.StandardSshClientProvider;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StringUtils;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.cipher.BuiltinCiphers;
import org.apache.sshd.common.kex.BuiltinDHFactories;
import org.apache.sshd.common.mac.BuiltinMacs;
import org.apache.sshd.common.signature.BuiltinSignatures;
import org.apache.sshd.sftp.client.SftpClient;
import org.apache.sshd.sftp.client.SftpClientFactory;
import org.apache.sshd.sftp.common.SftpConstants;
import org.apache.sshd.sftp.common.SftpException;
import org.apache.sshd.sftp.common.SftpHelper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SFTPTransfer implements FileTransfer {
    private static final SshClientProvider CLIENT_PROVIDER = new StandardSshClientProvider();

    private static final String DOT_PREFIX = ".";
    private static final String RELATIVE_CURRENT_DIRECTORY = DOT_PREFIX;
    private static final String RELATIVE_PARENT_DIRECTORY = "..";

    private static final Set<String> DEFAULT_KEY_ALGORITHM_NAMES = BuiltinSignatures.VALUES.stream()
            .map(BuiltinSignatures::getName)
            .collect(Collectors.toSet());
    private static final Set<String> DEFAULT_CIPHER_NAMES = BuiltinCiphers.VALUES.stream()
            .map(BuiltinCiphers::getName)
            .collect(Collectors.toSet());
    private static final Set<String> DEFAULT_MESSAGE_AUTHENTICATION_CODE_NAMES = BuiltinMacs.VALUES.stream()
            .map(BuiltinMacs::getName)
            .collect(Collectors.toSet());
    private static final Set<String> DEFAULT_KEY_EXCHANGE_ALGORITHM_NAMES = BuiltinDHFactories.VALUES.stream()
            .map(BuiltinDHFactories::getName)
            .collect(Collectors.toSet());

    /**
     * Converts a set of names into an alphabetically ordered comma separated value list.
     *
     * @param factorySetNames The set of names
     * @return An alphabetically ordered comma separated value list of names
     */
    private static String convertFactorySetToString(Set<String> factorySetNames) {
        return factorySetNames
                .stream()
                .sorted()
                .collect(Collectors.joining(", "));
    }

    private static String buildFullPath(String path, String filename) {
        if (path == null) {
            return filename;
        }

        if (path.endsWith("/")) {
            return path + filename;
        }

        return path + "/" + filename;
    }

    public static final PropertyDescriptor PRIVATE_KEY_PATH = new PropertyDescriptor.Builder()
        .name("Private Key Path")
        .description("The fully qualified path to the Private Key file")
        .required(false)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    public static final PropertyDescriptor PRIVATE_KEY_PASSPHRASE = new PropertyDescriptor.Builder()
        .name("Private Key Passphrase")
        .description("Password for the private key")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .sensitive(true)
        .build();
    public static final PropertyDescriptor HOST_KEY_FILE = new PropertyDescriptor.Builder()
        .name("Host Key File")
        .description("If supplied, the given file will be used as the Host Key;" +
                " otherwise, if 'Strict Host Key Checking' property is applied (set to true)" +
                " then uses the 'known_hosts' and 'known_hosts2' files from ~/.ssh directory" +
                " else no host key file will be used")
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
        .required(false)
        .build();
    public static final PropertyDescriptor STRICT_HOST_KEY_CHECKING = new PropertyDescriptor.Builder()
        .name("Strict Host Key Checking")
        .description("Indicates whether or not strict enforcement of hosts keys should be applied")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
        .name("Port")
        .description("The port that the remote system is listening on for file transfers")
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .defaultValue("22")
        .build();
    public static final PropertyDescriptor USE_KEEPALIVE_ON_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Send Keep Alive On Timeout")
        .description("Send a Keep Alive message every 5 seconds up to 5 times for an overall timeout of 25 seconds.")
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();

    public static final PropertyDescriptor ALGORITHM_CONFIGURATION = new PropertyDescriptor.Builder()
            .name("Algorithm Negotiation")
            .description("Configuration strategy for SSH algorithm negotiation")
            .required(true)
            .allowableValues(AlgorithmConfiguration.class)
            .defaultValue(AlgorithmConfiguration.DEFAULT)
            .build();

    public static final PropertyDescriptor KEY_ALGORITHMS_ALLOWED = new PropertyDescriptor.Builder()
            .name("Key Algorithms Allowed")
            .description("A comma-separated list of Key Algorithms allowed for SFTP connections. Leave unset to allow all. Available options are: "
                    + convertFactorySetToString(DEFAULT_KEY_ALGORITHM_NAMES))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(ALGORITHM_CONFIGURATION, AlgorithmConfiguration.CUSTOM)
            .build();

    public static final PropertyDescriptor CIPHERS_ALLOWED = new PropertyDescriptor.Builder()
            .name("Ciphers Allowed")
            .description("A comma-separated list of Ciphers allowed for SFTP connections. Leave unset to allow all. Available options are: " + convertFactorySetToString(DEFAULT_CIPHER_NAMES))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(ALGORITHM_CONFIGURATION, AlgorithmConfiguration.CUSTOM)
            .build();

    public static final PropertyDescriptor MESSAGE_AUTHENTICATION_CODES_ALLOWED = new PropertyDescriptor.Builder()
            .name("Message Authentication Codes Allowed")
            .description("A comma-separated list of Message Authentication Codes allowed for SFTP connections. Leave unset to allow all. Available options are: "
                    + convertFactorySetToString(DEFAULT_MESSAGE_AUTHENTICATION_CODE_NAMES))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(ALGORITHM_CONFIGURATION, AlgorithmConfiguration.CUSTOM)
            .build();

    public static final PropertyDescriptor KEY_EXCHANGE_ALGORITHMS_ALLOWED = new PropertyDescriptor.Builder()
            .name("Key Exchange Algorithms Allowed")
            .description("A comma-separated list of Key Exchange Algorithms allowed for SFTP connections. Leave unset to allow all. Available options are: "
                    + convertFactorySetToString(DEFAULT_KEY_EXCHANGE_ALGORITHM_NAMES))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(ALGORITHM_CONFIGURATION, AlgorithmConfiguration.CUSTOM)
            .build();

    /**
     * Property which is used to decide if the {@link #ensureDirectoryExists(FlowFile, File)} method should perform a list before calling mkdir.
     * In most cases, the code should call ls before mkdir, but some weird permission setups (chmod 100) on a directory would cause the 'ls' to throw a permission
     * exception.
     */
    public static final PropertyDescriptor DISABLE_DIRECTORY_LISTING = new PropertyDescriptor.Builder()
        .name("Disable Directory Listing")
        .description("If set to 'true', directory listing is not performed prior to create missing directories." +
                " By default, this processor executes a directory listing command" +
                " to see target directory existence before creating missing directories." +
                " However, there are situations that you might need to disable the directory listing such as the following." +
                " Directory listing might fail with some permission setups (e.g. chmod 100) on a directory." +
                " Also, if any other SFTP client created the directory after this processor performed a listing" +
                " and before a directory creation request by this processor is finished," +
                " then an error is returned because the directory already exists.")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

    public enum AlgorithmConfiguration implements DescribedValue {
        DEFAULT("Default algorithm negotiation based on standard settings for general compatibility with SSH servers"),

        CUSTOM("Custom algorithm negotiation based on defined settings");

        private final String description;

        AlgorithmConfiguration(final String description) {
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return name();
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH, ProxySpec.SOCKS_AUTH};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(PROXY_SPECS);

    private final ComponentLog logger;

    private final PropertyContext ctx;

    private ClientSession clientSession;
    private SftpClient sftpClient;

    private volatile boolean closed = false;
    private String homeDir;
    private String activeHostname;
    private String activePort;
    private String activeUsername;
    private String activePassword;
    private String activePrivateKeyPath;
    private String activePrivateKeyPassphrase;

    private final boolean disableDirectoryListing;

    public SFTPTransfer(final PropertyContext propertyContext, final ComponentLog logger) {
        this.ctx = propertyContext;
        this.logger = logger;

        final PropertyValue disableListing = propertyContext.getProperty(DISABLE_DIRECTORY_LISTING);
        disableDirectoryListing = disableListing != null && Boolean.TRUE.equals(disableListing.asBoolean());
    }

    public static void migrateAlgorithmProperties(final PropertyConfiguration propertyConfiguration) {
        if (!propertyConfiguration.hasProperty(ALGORITHM_CONFIGURATION)) {
            final boolean customAlgorithmConfiguration =
                    propertyConfiguration.isPropertySet(KEY_ALGORITHMS_ALLOWED)
                            || propertyConfiguration.isPropertySet(CIPHERS_ALLOWED)
                            || propertyConfiguration.isPropertySet(MESSAGE_AUTHENTICATION_CODES_ALLOWED)
                            || propertyConfiguration.isPropertySet(KEY_EXCHANGE_ALGORITHMS_ALLOWED);
            if (customAlgorithmConfiguration) {
                propertyConfiguration.setProperty(ALGORITHM_CONFIGURATION, AlgorithmConfiguration.CUSTOM.getValue());
            }
        }
    }

    public static void validateProxySpec(ValidationContext context, Collection<ValidationResult> results) {
        ProxyConfiguration.validateProxySpec(context, results, PROXY_SPECS);
    }

    @Override
    public String getProtocolName() {
        return "sftp";
    }

    @Override
    public List<FileInfo> getListing(final boolean applyFilters) throws IOException {
        final String path = ctx.getProperty(FileTransfer.REMOTE_PATH).evaluateAttributeExpressions().getValue();
        final int depth = 0;

        final int maxResults;
        final PropertyValue batchSizeValue = ctx.getProperty(FileTransfer.REMOTE_POLL_BATCH_SIZE);
        if (batchSizeValue == null) {
            maxResults = Integer.MAX_VALUE;
        } else {
            final Integer configuredValue = batchSizeValue.asInteger();
            maxResults = configuredValue == null ? Integer.MAX_VALUE : configuredValue;
        }

        final List<FileInfo> listing = new ArrayList<>(1000);
        getListing(path, depth, maxResults, listing, applyFilters);

        return listing;
    }

    protected void getListing(
            final String path,
            final int depth,
            final int maxResults,
            final List<FileInfo> listing,
            final boolean applyFilters
    ) throws IOException {
        if (maxResults < 1 || listing.size() >= maxResults) {
            return;
        }

        if (depth >= 100) {
            logger.warn("{} had to stop recursively searching directories at a recursive depth of {} to avoid memory issues", this, depth);
            return;
        }

        final boolean ignoreDottedFiles = ctx.getProperty(FileTransfer.IGNORE_DOTTED_FILES).asBoolean();
        final boolean recurse = ctx.getProperty(FileTransfer.RECURSIVE_SEARCH).asBoolean();
        final boolean symlink  = ctx.getProperty(FileTransfer.FOLLOW_SYMLINK).asBoolean();
        final String fileFilterRegex = ctx.getProperty(FileTransfer.FILE_FILTER_REGEX).getValue();
        final Pattern fileFilterPattern = (fileFilterRegex == null) ? null : Pattern.compile(fileFilterRegex);
        final String pathFilterRegex = ctx.getProperty(FileTransfer.PATH_FILTER_REGEX).getValue();
        final Pattern pathPattern = (!recurse || pathFilterRegex == null) ? null : Pattern.compile(pathFilterRegex);
        final String remotePath = ctx.getProperty(FileTransfer.REMOTE_PATH).evaluateAttributeExpressions().getValue();

        // check if this directory path matches the PATH_FILTER_REGEX
        boolean pathFilterMatches = true;
        if (pathPattern != null) {
            Path relativeDir = path == null ? Paths.get(RELATIVE_CURRENT_DIRECTORY) : Paths.get(path);
            if (remotePath != null) {
                relativeDir = Paths.get(remotePath).relativize(relativeDir);
            }
            if (!relativeDir.toString().isEmpty()) {
                if (!pathPattern.matcher(relativeDir.toString().replace("\\", "/")).matches()) {
                    pathFilterMatches = false;
                }
            }
        }

        final SftpClient sftpClient = getSFTPClient(null);
        final boolean pathMatched = pathFilterMatches;
        final boolean filteringDisabled = !applyFilters;

        final List<SftpClient.DirEntry> subDirectoryPaths = new ArrayList<>();
        try {
            final String directory;
            if (path == null || path.isBlank()) {
                directory = RELATIVE_CURRENT_DIRECTORY;
            } else {
                directory = path;
            }

            for (final SftpClient.DirEntry dirEntry : sftpClient.readDir(directory)) {
                final String entryFilename = dirEntry.getFilename();

                if (RELATIVE_CURRENT_DIRECTORY.equals(entryFilename) || RELATIVE_PARENT_DIRECTORY.equals(entryFilename)) {
                    continue;
                }

                if (ignoreDottedFiles && entryFilename.startsWith(DOT_PREFIX)) {
                    continue;
                }

                // remember directory for later recursive listing
                if (isIncludedDirectory(dirEntry, recurse, symlink)) {
                    subDirectoryPaths.add(dirEntry);
                    continue;
                }

                // add regular files matching our filter to the result
                if (isIncludedFile(dirEntry, symlink) && (filteringDisabled || pathMatched)) {
                    if (filteringDisabled || fileFilterPattern == null || fileFilterPattern.matcher(entryFilename).matches()) {
                        listing.add(newFileInfo(path, entryFilename, dirEntry.getAttributes()));

                        // abort further processing once we've reached the configured amount of maxResults
                        if (listing.size() >= maxResults) {
                            break;
                        }
                    }
                }
            }
        } catch (final UncheckedIOException | SftpException e) {
            final String resolvedPath = path == null ? "current directory" : path;

            final int status;
            if (e instanceof SftpException sftpException) {
                status = sftpException.getStatus();
            } else if (e.getCause() instanceof SftpException sftpException) {
                status = sftpException.getStatus();
            } else {
                status = SftpConstants.SSH_FX_FAILURE;
            }

            switch (status) {
                case SftpConstants.SSH_FX_INVALID_HANDLE:
                case SftpConstants.SSH_FX_NO_SUCH_FILE:
                    throw new FileNotFoundException("No such file or directory [%s] on remote system".formatted(resolvedPath));
                case SftpConstants.SSH_FX_PERMISSION_DENIED:
                    throw new PermissionDeniedException("Insufficient permissions to read directory [%s]".formatted(resolvedPath), e);
                default:
                    throw new IOException("Failed to read directory [%s]".formatted(resolvedPath), e);
            }
        }

        for (final SftpClient.DirEntry dirEntry : subDirectoryPaths) {
            final String entryFilename = dirEntry.getFilename();
            final File newFullPath = new File(path, entryFilename);
            final String newFullForwardPath = newFullPath.getPath().replace("\\", "/");

            try {
                getListing(newFullForwardPath, depth + 1, maxResults, listing, applyFilters);
            } catch (final IOException e) {
                logger.error("Unable to get listing from {}; skipping", newFullForwardPath, e);
            }
        }
    }

    /**
     * Include remote resources when regular file found or when symbolic links are enabled and the resource is a link
     *
     * @param dirEntry Remote Directory Entry
     * @param symlinksEnabled Follow symbolic links enabled
     * @return Included file status
     */
    private boolean isIncludedFile(final SftpClient.DirEntry dirEntry, final boolean symlinksEnabled) {
        final SftpClient.Attributes attributes = dirEntry.getAttributes();
        return attributes.isRegularFile() || attributes.isOther() || (symlinksEnabled && attributes.isSymbolicLink());
    }

    /**
     * Include remote resources when recursion is enabled or when symbolic links are enabled and the resource is a directory link
     *
     * @param dirEntry Remote Directory Entry
     * @param recursionEnabled Recursion enabled status
     * @param symlinksEnabled Follow symbolic links enabled
     * @return Included directory status
     */
    private boolean isIncludedDirectory(final SftpClient.DirEntry dirEntry, final boolean recursionEnabled, final boolean symlinksEnabled) {
        boolean includedDirectory = false;

        final SftpClient.Attributes entryAttributes = dirEntry.getAttributes();
        if (entryAttributes.isDirectory()) {
            includedDirectory = recursionEnabled;
        } else if (symlinksEnabled && entryAttributes.isSymbolicLink()) {
            final String path = dirEntry.getFilename();
            try {
                final SftpClient.Attributes attributes = sftpClient.stat(path);
                includedDirectory = attributes.isDirectory();
            } catch (final IOException e) {
                logger.warn("Read symbolic link attributes failed [{}]", path, e);
            }
        }

        return includedDirectory;
    }

    private FileInfo newFileInfo(String path, String filename, final SftpClient.Attributes attributes) {
        final File newFullPath = new File(path, filename);
        final String newFullForwardPath = newFullPath.getPath().replace("\\", "/");

        final StringBuilder permsBuilder = new StringBuilder();
        final Set<PosixFilePermission> filePermissions = SftpHelper.permissionsToAttributes(attributes.getPermissions());

        appendPermission(permsBuilder, filePermissions, PosixFilePermission.OWNER_READ, "r");
        appendPermission(permsBuilder, filePermissions, PosixFilePermission.OWNER_WRITE, "w");
        appendPermission(permsBuilder, filePermissions, PosixFilePermission.OWNER_EXECUTE, "x");

        appendPermission(permsBuilder, filePermissions, PosixFilePermission.GROUP_READ, "r");
        appendPermission(permsBuilder, filePermissions, PosixFilePermission.GROUP_WRITE, "w");
        appendPermission(permsBuilder, filePermissions, PosixFilePermission.GROUP_EXECUTE, "x");

        appendPermission(permsBuilder, filePermissions, PosixFilePermission.OTHERS_READ, "r");
        appendPermission(permsBuilder, filePermissions, PosixFilePermission.OTHERS_WRITE, "w");
        appendPermission(permsBuilder, filePermissions, PosixFilePermission.OTHERS_EXECUTE, "x");

        final FileInfo.Builder builder = new FileInfo.Builder()
            .filename(filename)
            .fullPathFileName(newFullForwardPath)
            .directory(attributes.isDirectory())
            .size(attributes.getSize())
            .lastModifiedTime(attributes.getModifyTime().toMillis())
            .permissions(permsBuilder.toString())
            .owner(Integer.toString(attributes.getUserId()))
            .group(Integer.toString(attributes.getGroupId()));
        return builder.build();
    }

    private void appendPermission(final StringBuilder builder, final Set<PosixFilePermission> permissions, final PosixFilePermission filePermission, final String permString) {
        if (permissions.contains(filePermission)) {
            builder.append(permString);
        } else {
            builder.append("-");
        }
    }

    @Override
    public FlowFile getRemoteFile(final String remoteFileName, final FlowFile origFlowFile, final ProcessSession session) throws ProcessException, IOException {
        final SftpClient sftpClient = getSFTPClient(origFlowFile);

        try (InputStream inputStream = sftpClient.read(remoteFileName)) {
            return session.write(origFlowFile, out -> StreamUtils.copy(inputStream, out));
        } catch (final SftpException e) {
            final int status = e.getStatus();
            switch (status) {
                case SftpConstants.SSH_FX_NO_SUCH_FILE:
                    throw new FileNotFoundException("No such file or directory [%s] on remote system".formatted(remoteFileName));
                case SftpConstants.SSH_FX_PERMISSION_DENIED:
                    throw new PermissionDeniedException("Insufficient permissions to read [%s]".formatted(remoteFileName), e);
                default:
                    throw new IOException("Failed to read [%s]".formatted(remoteFileName), e);
            }
        }
    }

    @Override
    public void deleteFile(final FlowFile flowFile, final String path, final String remoteFileName) throws IOException {
        final SftpClient sftpClient = getSFTPClient(flowFile);
        final String fullPath = buildFullPath(path, remoteFileName);
        try {
            sftpClient.remove(fullPath);
        } catch (final SftpException e) {
            final int status = e.getStatus();
            switch (status) {
                case SftpConstants.SSH_FX_NO_SUCH_FILE:
                    throw new FileNotFoundException("No such file or directory [%s] on remote system".formatted(fullPath));
                case SftpConstants.SSH_FX_PERMISSION_DENIED:
                    throw new PermissionDeniedException("Insufficient permissions to delete [%s]".formatted(fullPath), e);
                default:
                    throw new IOException("Failed to delete [%s]".formatted(fullPath), e);
            }
        }
    }

    @Override
    public void deleteDirectory(final FlowFile flowFile, final String remoteDirectoryName) throws IOException {
        final SftpClient sftpClient = getSFTPClient(flowFile);
        sftpClient.rmdir(remoteDirectoryName);
    }

    @Override
    public void ensureDirectoryExists(final FlowFile flowFile, final File directoryName) throws IOException {
        final SftpClient sftpClient = getSFTPClient(flowFile);
        final String remoteDirectory = directoryName.getAbsolutePath().replace("\\", "/").replaceAll("^.:", "");

        // if we disable the directory listing, we just want to blindly perform the mkdir command,
        // eating failure exceptions thrown (like if the directory already exists).
        if (disableDirectoryListing) {
            try {
                // Blindly create the dir.
                sftpClient.mkdir(remoteDirectory);
                // The remote directory did not exist, and was created successfully.
                return;
            } catch (final SftpException e) {
                final int status = e.getStatus();
                final String statusName = SftpConstants.getStatusName(status);
                if (SftpConstants.SSH_FX_NO_SUCH_FILE == status) {
                    logger.debug("Failed to create directory [{}] Status [{}] attempting to create parent directory", directoryName, statusName);
                } else if (SftpConstants.SSH_FX_FAILURE == status) {
                    logger.debug("Failed to create directory [{}] Status [{}]", remoteDirectory, statusName);
                    return;
                } else {
                    throw new IOException("Failed to create directory [%s] Status [%s]".formatted(remoteDirectory, statusName), e);
                }
            }
        } else {
            try {
                // Check dir existence.
                sftpClient.stat(remoteDirectory);
                // The remote directory already exists.
                return;
            } catch (final SftpException e) {
                final int status = e.getStatus();
                if (SftpConstants.SSH_FX_NO_SUCH_FILE != status) {
                    throw new IOException("Failed to determine remote directory existence [%s]".formatted(remoteDirectory), e);
                }
            }
        }

        // first ensure parent directories exist before creating this one
        if (directoryName.getParent() != null && !directoryName.getParentFile().equals(new File(File.separator))) {
            ensureDirectoryExists(flowFile, directoryName.getParentFile());
        }
        logger.debug("Remote Directory {} does not exist; creating it", remoteDirectory);
        sftpClient.mkdir(remoteDirectory);
        logger.debug("Created {}", remoteDirectory);
    }

    protected SftpClient getSFTPClient(final FlowFile flowFile) throws IOException {
        final String evaledHostname = ctx.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
        final String evaledPort = ctx.getProperty(PORT).evaluateAttributeExpressions(flowFile).getValue();
        final String evaledUsername = ctx.getProperty(USERNAME).evaluateAttributeExpressions(flowFile).getValue();
        final String evaledPassword = ctx.getProperty(PASSWORD).evaluateAttributeExpressions(flowFile).getValue();
        final String evaledPrivateKeyPath = ctx.getProperty(PRIVATE_KEY_PATH).evaluateAttributeExpressions(flowFile).getValue();
        final String evaledPrivateKeyPassphrase = ctx.getProperty(PRIVATE_KEY_PASSPHRASE).evaluateAttributeExpressions(flowFile).getValue();

        // If the client is already initialized then compare the host that the client is connected to with the current
        // host from the properties/flow-file, and if different then we need to close and reinitialize, if same we can reuse
        if (sftpClient != null) {
            if (Objects.equals(evaledHostname, activeHostname)
                    && Objects.equals(evaledPort, activePort)
                    && Objects.equals(evaledUsername, activeUsername)
                    && Objects.equals(evaledPassword, activePassword)
                    && Objects.equals(evaledPrivateKeyPath, activePrivateKeyPath)
                    && Objects.equals(evaledPrivateKeyPassphrase, activePrivateKeyPassphrase)
            ) {
                // destination matches so we can keep our current session
                return sftpClient;
            } else {
                // this flowFile is going to a different destination, reset session
                close();
            }
        }

        final Map<String, String> attributes = flowFile == null ? Collections.emptyMap() : flowFile.getAttributes();
        this.clientSession = CLIENT_PROVIDER.getClientSession(ctx, attributes);

        final SftpClientFactory sftpClientFactory = SftpClientFactory.instance();
        sftpClient = sftpClientFactory.createSftpClient(clientSession);

        activeHostname = evaledHostname;
        activePort = evaledPort;
        activePassword = evaledPassword;
        activeUsername = evaledUsername;
        activePrivateKeyPath = evaledPrivateKeyPath;
        activePrivateKeyPassphrase = evaledPrivateKeyPassphrase;
        this.closed = false;

        try {
            this.homeDir = sftpClient.canonicalPath("");
        } catch (final IOException e) {
            this.homeDir = "";
            // For some combination of server configuration and user home directory, getHome() can fail with "2: File not found"
            // Since  homeDir is only used tor SEND provenance event transit uri, this is harmless. Log and continue.
            logger.debug("Failed to retrieve home directory due to {}", e.getMessage());
        }

        return sftpClient;
    }

    @Override
    public String getHomeDirectory(final FlowFile flowFile) throws IOException {
        getSFTPClient(flowFile);
        return this.homeDir;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        try {
            if (null != sftpClient) {
                sftpClient.close();
            }
        } catch (final Exception ex) {
            logger.warn("Failed to close SFTPClient", ex);
        }
        sftpClient = null;

        try {
            if (clientSession != null) {
                clientSession.close();
            }
        } catch (final Exception e) {
            logger.warn("Failed to close SSH Client", e);
        }
        clientSession = null;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public FileInfo getRemoteFileInfo(final FlowFile flowFile, final String path, String filename) throws IOException {
        final SftpClient sftpClient = getSFTPClient(flowFile);

        final FileInfo fileInfo;

        final String fullPath = buildFullPath(path, filename);
        try {
            final SftpClient.Attributes fileAttributes = sftpClient.stat(fullPath);
            if (fileAttributes.isDirectory()) {
                fileInfo = null;
            } else {
                fileInfo = newFileInfo(path, filename, fileAttributes);
            }
        } catch (final SftpException e) {
            final int status = e.getStatus();
            if (SftpConstants.SSH_FX_NO_SUCH_FILE == status) {
                return null;
            } else {
                throw new IOException("Failed to read remote attributes [%s]".formatted(fullPath), e);
            }
        }

        return fileInfo;
    }

    @Override
    public String put(final FlowFile flowFile, final String path, final String filename, final InputStream content) throws IOException {
        final SftpClient sftpClient = getSFTPClient(flowFile);

        // destination path + filename
        final String fullPath = buildFullPath(path, filename);

        // temporary path + filename
        String tempFilename = ctx.getProperty(TEMP_FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        if (tempFilename == null) {
            final boolean dotRename = ctx.getProperty(DOT_RENAME).asBoolean();
            tempFilename = dotRename ? DOT_PREFIX + filename : filename;
        }
        final String tempPath = buildFullPath(path, tempFilename);

        try {
            sftpClient.put(content, tempPath);
        } catch (final SftpException e) {
            throw new IOException("Failed to transfer content to [%s]".formatted(fullPath), e);
        }

        setAttributes(flowFile, tempPath);

        if (!filename.equals(tempFilename)) {
            try {
                // file was transferred to a temporary filename, attempt to delete destination filename before rename
                sftpClient.remove(fullPath);
            } catch (final SftpException e) {
                logger.debug("Failed to remove {} before renaming temporary file", fullPath, e);
            }

            try {
                sftpClient.rename(tempPath, fullPath);
            } catch (final SftpException e) {
                try {
                    sftpClient.remove(tempPath);
                    throw new IOException("Failed to rename temporary file to [%s]".formatted(fullPath), e);
                } catch (final SftpException removeException) {
                    throw new IOException("Failed to rename temporary file to [%s] and removal failed".formatted(fullPath), removeException);
                }
            }
        }

        return fullPath;
    }

    private void setAttributes(final FlowFile flowFile, final String remotePath) {
        final AttributesRequested attributesRequested = new AttributesRequested();

        final String permissions = ctx.getProperty(PERMISSIONS).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isNotEmpty(permissions)) {
            final int perms = numberPermissions(permissions);
            attributesRequested.setPermissions(perms);
        }

        final String lastModifiedTime = ctx.getProperty(LAST_MODIFIED_TIME).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isNotBlank(lastModifiedTime)) {
            final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
            final OffsetDateTime offsetDateTime = OffsetDateTime.parse(lastModifiedTime, dateTimeFormatter);
            final FileTime modifyTime = FileTime.from(offsetDateTime.toInstant());
            attributesRequested.setModifyTime(modifyTime);
        }

        final String owner = ctx.getProperty(REMOTE_OWNER).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isNotEmpty(owner)) {
            attributesRequested.setOwner(owner);
        }

        final String group = ctx.getProperty(REMOTE_GROUP).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isNotEmpty(group)) {
            attributesRequested.setGroup(group);
        }

        if (attributesRequested.isConfigured()) {
            try {
                final SftpClient.Attributes attributes = sftpClient.stat(remotePath);
                attributesRequested.setAttributes(attributes);
                sftpClient.setStat(remotePath, attributes);
            } catch (final IOException e) {
                logger.warn("Failed to set attributes on Remote File [{}] for {}", remotePath, flowFile, e);
            }
        }
    }

    @Override
    public void rename(final FlowFile flowFile, final String source, final String target) throws IOException {
        final SftpClient sftpClient = getSFTPClient(flowFile);
        try {
            sftpClient.rename(source, target);
        } catch (final SftpException e) {
            final int status = e.getStatus();
            switch (status) {
                case SftpConstants.SSH_FX_NO_SUCH_FILE:
                    throw new FileNotFoundException("No such file or directory [%s] on remote system".formatted(source));
                case SftpConstants.SSH_FX_PERMISSION_DENIED:
                    throw new PermissionDeniedException("Insufficient permissions to rename [%s] to [%s]".formatted(source, target), e);
                default:
                    throw new IOException("Failed to rename [%s] to [%s]".formatted(source, target), e);
            }
        }
    }

    protected int numberPermissions(String perms) {
        int number = -1;
        final Pattern rwxPattern = Pattern.compile("^[rwx-]{9}$");
        final Pattern numPattern = Pattern.compile("\\d+");
        if (rwxPattern.matcher(perms).matches()) {
            number = 0;
            if (perms.charAt(0) == 'r') {
                number |= 0x100;
            }
            if (perms.charAt(1) == 'w') {
                number |= 0x80;
            }
            if (perms.charAt(2) == 'x') {
                number |= 0x40;
            }
            if (perms.charAt(3) == 'r') {
                number |= 0x20;
            }
            if (perms.charAt(4) == 'w') {
                number |= 0x10;
            }
            if (perms.charAt(5) == 'x') {
                number |= 0x8;
            }
            if (perms.charAt(6) == 'r') {
                number |= 0x4;
            }
            if (perms.charAt(7) == 'w') {
                number |= 0x2;
            }
            if (perms.charAt(8) == 'x') {
                number |= 0x1;
            }
        } else if (numPattern.matcher(perms).matches()) {
            try {
                number = Integer.parseInt(perms, 8);
            } catch (NumberFormatException ignored) {
            }
        }
        return number;
    }

    private static class AttributesRequested {
        private boolean configured;

        private Integer permissions;

        private FileTime modifyTime;

        private String owner;

        private String group;

        void setPermissions(final int permissions) {
            this.permissions = permissions;
            this.configured = true;
        }

        void setModifyTime(final FileTime modifyTime) {
            this.modifyTime = modifyTime;
            this.configured = true;
        }

        void setOwner(final String owner) {
            this.owner = owner;
            this.configured = true;
        }

        void setGroup(final String group) {
            this.group = group;
            this.configured = true;
        }

        boolean isConfigured() {
            return configured;
        }

        void setAttributes(final SftpClient.Attributes attributes) {
            if (permissions != null) {
                attributes.setPermissions(permissions);
            }
            if (modifyTime != null) {
                attributes.setModifyTime(modifyTime);
            }
            if (StringUtils.isNotBlank(owner)) {
                attributes.setOwner(owner);
            }
            if (StringUtils.isNotBlank(group)) {
                attributes.setGroup(group);
            }
        }
    }
}
