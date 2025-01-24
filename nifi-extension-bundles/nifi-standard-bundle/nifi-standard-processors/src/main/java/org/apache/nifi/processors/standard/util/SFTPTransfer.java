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

import com.hierynomus.sshj.sftp.RemoteResourceSelector;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.Factory;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.FileMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.Response;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPEngine;
import net.schmizz.sshj.sftp.SFTPException;
import net.schmizz.sshj.xfer.FilePermission;
import net.schmizz.sshj.xfer.LocalSourceFile;
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
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processor.util.file.transfer.PermissionDeniedException;
import org.apache.nifi.processors.standard.ssh.SSHClientProvider;
import org.apache.nifi.processors.standard.ssh.StandardSSHClientProvider;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SFTPTransfer implements FileTransfer {
    private static final SSHClientProvider SSH_CLIENT_PROVIDER = new StandardSSHClientProvider();

    private static final String DOT_PREFIX = ".";
    private static final String RELATIVE_CURRENT_DIRECTORY = DOT_PREFIX;
    private static final String RELATIVE_PARENT_DIRECTORY = "..";

    private static final Set<String> DEFAULT_KEY_ALGORITHM_NAMES;
    private static final Set<String> DEFAULT_CIPHER_NAMES;
    private static final Set<String> DEFAULT_MESSAGE_AUTHENTICATION_CODE_NAMES;
    private static final Set<String> DEFAULT_KEY_EXCHANGE_ALGORITHM_NAMES;

    static {
        DefaultConfig defaultConfig = new DefaultConfig();

        DEFAULT_KEY_ALGORITHM_NAMES = defaultConfig.getKeyAlgorithms().stream()
                .map(Factory.Named::getName).collect(Collectors.toUnmodifiableSet());
        DEFAULT_CIPHER_NAMES = defaultConfig.getCipherFactories().stream()
                .map(Factory.Named::getName).collect(Collectors.toUnmodifiableSet());
        DEFAULT_MESSAGE_AUTHENTICATION_CODE_NAMES = defaultConfig.getMACFactories().stream()
                .map(Factory.Named::getName).collect(Collectors.toUnmodifiableSet());
        DEFAULT_KEY_EXCHANGE_ALGORITHM_NAMES = defaultConfig.getKeyExchangeFactories().stream()
                .map(Factory.Named::getName).collect(Collectors.toUnmodifiableSet());
    }

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

    private static boolean isDirectory(FileAttributes attributes) {
        return attributes.getType() == FileMode.Type.DIRECTORY;
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

    public static final PropertyDescriptor KEY_ALGORITHMS_ALLOWED = new PropertyDescriptor.Builder()
            .name("Key Algorithms Allowed")
            .displayName("Key Algorithms Allowed")
            .description("A comma-separated list of Key Algorithms allowed for SFTP connections. Leave unset to allow all. Available options are: "
                    + convertFactorySetToString(DEFAULT_KEY_ALGORITHM_NAMES))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CIPHERS_ALLOWED = new PropertyDescriptor.Builder()
            .name("Ciphers Allowed")
            .displayName("Ciphers Allowed")
            .description("A comma-separated list of Ciphers allowed for SFTP connections. Leave unset to allow all. Available options are: " + convertFactorySetToString(DEFAULT_CIPHER_NAMES))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor MESSAGE_AUTHENTICATION_CODES_ALLOWED = new PropertyDescriptor.Builder()
            .name("Message Authentication Codes Allowed")
            .displayName("Message Authentication Codes Allowed")
            .description("A comma-separated list of Message Authentication Codes allowed for SFTP connections. Leave unset to allow all. Available options are: "
                    + convertFactorySetToString(DEFAULT_MESSAGE_AUTHENTICATION_CODE_NAMES))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_EXCHANGE_ALGORITHMS_ALLOWED = new PropertyDescriptor.Builder()
            .name("Key Exchange Algorithms Allowed")
            .displayName("Key Exchange Algorithms Allowed")
            .description("A comma-separated list of Key Exchange Algorithms allowed for SFTP connections. Leave unset to allow all. Available options are: "
                    + convertFactorySetToString(DEFAULT_KEY_EXCHANGE_ALGORITHM_NAMES))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    /**
     * Property which is used to decide if the {@link #ensureDirectoryExists(FlowFile, File)} method should perform a {@link SFTPClient#ls(String)} before calling
     * {@link SFTPClient#mkdir(String)}. In most cases, the code should call ls before mkdir, but some weird permission setups (chmod 100) on a directory would cause the 'ls' to throw a permission
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

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH, ProxySpec.SOCKS_AUTH};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(PROXY_SPECS);

    private final ComponentLog logger;

    private final PropertyContext ctx;

    private SSHClient sshClient;
    private SFTPClient sftpClient;

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
        disableDirectoryListing = disableListing == null ? false : Boolean.TRUE.equals(disableListing.asBoolean());
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

    protected void getListing(final String path, final int depth, final int maxResults, final List<FileInfo> listing,
                              final boolean applyFilters) throws IOException {
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
            Path reldir = path == null ? Paths.get(RELATIVE_CURRENT_DIRECTORY) : Paths.get(path);
            if (remotePath != null) {
                reldir = Paths.get(remotePath).relativize(reldir);
            }
            if (reldir != null && !reldir.toString().isEmpty()) {
                if (!pathPattern.matcher(reldir.toString().replace("\\", "/")).matches()) {
                    pathFilterMatches = false;
                }
            }
        }

        final SFTPClient sftpClient = getSFTPClient(null);
        final boolean pathMatched = pathFilterMatches;
        final boolean filteringDisabled = !applyFilters;

        final List<RemoteResourceInfo> subDirectoryPaths = new ArrayList<>();
        try {
            final RemoteResourceSelector selector = (entry) -> {
                final String entryFilename = entry.getName();

                // skip over 'this directory' and 'parent directory' special files regardless of ignoring dot files
                if (RELATIVE_CURRENT_DIRECTORY.equals(entryFilename) || RELATIVE_PARENT_DIRECTORY.equals(entryFilename)) {
                    return RemoteResourceSelector.Result.CONTINUE;
                }

                // skip files and directories that begin with a dot if we're ignoring them
                if (ignoreDottedFiles && entryFilename.startsWith(DOT_PREFIX)) {
                    return RemoteResourceSelector.Result.CONTINUE;
                }

                // remember directory for later recursive listing
                if (isIncludedDirectory(entry, recurse, symlink)) {
                    subDirectoryPaths.add(entry);
                    return RemoteResourceSelector.Result.CONTINUE;
                }

                // add regular files matching our filter to the result
                if (isIncludedFile(entry, symlink) && (filteringDisabled || pathMatched)) {
                    if (filteringDisabled || fileFilterPattern == null || fileFilterPattern.matcher(entryFilename).matches()) {
                        listing.add(newFileInfo(path, entry.getName(), entry.getAttributes()));

                        // abort further processing once we've reached the configured amount of maxResults
                        if (listing.size() >= maxResults) {
                            return RemoteResourceSelector.Result.BREAK;
                        }
                    }
                }

                // SSHJ does not need to keep track as we collect the results ourselves, continue with next entry instead
                return RemoteResourceSelector.Result.CONTINUE;
            };

            if (path == null || path.isBlank()) {
                sftpClient.ls(RELATIVE_CURRENT_DIRECTORY, selector);
            } else {
                sftpClient.ls(path, selector);
            }
        } catch (final SFTPException e) {
            final String pathDesc = path == null ? "current directory" : path;
            switch (e.getStatusCode()) {
                case NO_SUCH_FILE:
                    throw new FileNotFoundException("Could not perform listing on " + pathDesc + " because could not find the file on the remote server");
                case PERMISSION_DENIED:
                    throw new PermissionDeniedException("Could not perform listing on " + pathDesc + " due to insufficient permissions");
                default:
                    throw new IOException(String.format("Failed to obtain file listing for %s due to unexpected SSH_FXP_STATUS (%d)",
                            pathDesc, e.getStatusCode().getCode()), e);
            }
        }

        for (final RemoteResourceInfo entry : subDirectoryPaths) {
            final String entryFilename = entry.getName();
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
     * @param remoteResourceInfo Remote Resource Information
     * @param symlinksEnabled Follow symbolic links enabled
     * @return Included file status
     */
    private boolean isIncludedFile(final RemoteResourceInfo remoteResourceInfo, final boolean symlinksEnabled) {
        return remoteResourceInfo.isRegularFile() || (remoteResourceInfo.getAttributes().getMode().getType() == FileMode.Type.UNKNOWN) || (symlinksEnabled && isSymlink(remoteResourceInfo));
    }

    /**
     * Include remote resources when recursion is enabled or when symbolic links are enabled and the resource is a directory link
     *
     * @param remoteResourceInfo Remote Resource Information
     * @param recursionEnabled Recursion enabled status
     * @param symlinksEnabled Follow symbolic links enabled
     * @return Included directory status
     */
    private boolean isIncludedDirectory(final RemoteResourceInfo remoteResourceInfo, final boolean recursionEnabled, final boolean symlinksEnabled) {
        boolean includedDirectory = false;

        if (remoteResourceInfo.isDirectory()) {
            includedDirectory = recursionEnabled;
        } else if (symlinksEnabled && isSymlink(remoteResourceInfo)) {
            final String path = remoteResourceInfo.getPath();
            try {
                final FileAttributes pathAttributes = sftpClient.stat(path);
                includedDirectory = FileMode.Type.DIRECTORY == pathAttributes.getMode().getType();
            } catch (final IOException e) {
                logger.warn("Read symbolic link attributes failed [{}]", path, e);
            }
        }

        return includedDirectory;
    }

    private boolean isSymlink(final RemoteResourceInfo remoteResourceInfo) {
        return FileMode.Type.SYMLINK == remoteResourceInfo.getAttributes().getType();
    }

    private FileInfo newFileInfo(String path, String filename, final FileAttributes attributes) {
        final File newFullPath = new File(path, filename);
        final String newFullForwardPath = newFullPath.getPath().replace("\\", "/");

        final StringBuilder permsBuilder = new StringBuilder();
        final Set<FilePermission> permissions = attributes.getPermissions();

        appendPermission(permsBuilder, permissions, FilePermission.USR_R, "r");
        appendPermission(permsBuilder, permissions, FilePermission.USR_W, "w");
        appendPermission(permsBuilder, permissions, FilePermission.USR_X, "x");

        appendPermission(permsBuilder, permissions, FilePermission.GRP_R, "r");
        appendPermission(permsBuilder, permissions, FilePermission.GRP_W, "w");
        appendPermission(permsBuilder, permissions, FilePermission.GRP_X, "x");

        appendPermission(permsBuilder, permissions, FilePermission.OTH_R, "r");
        appendPermission(permsBuilder, permissions, FilePermission.OTH_W, "w");
        appendPermission(permsBuilder, permissions, FilePermission.OTH_X, "x");

        final FileInfo.Builder builder = new FileInfo.Builder()
            .filename(filename)
            .fullPathFileName(newFullForwardPath)
            .directory(isDirectory(attributes))
            .size(attributes.getSize())
            .lastModifiedTime(attributes.getMtime() * 1000L)
            .permissions(permsBuilder.toString())
            .owner(Integer.toString(attributes.getUID()))
            .group(Integer.toString(attributes.getGID()));
        return builder.build();
    }

    private void appendPermission(final StringBuilder builder, final Set<FilePermission> permissions, final FilePermission filePermission, final String permString) {
        if (permissions.contains(filePermission)) {
            builder.append(permString);
        } else {
            builder.append("-");
        }
    }

    @Override
    public FlowFile getRemoteFile(final String remoteFileName, final FlowFile origFlowFile, final ProcessSession session) throws ProcessException, IOException {
        final SFTPClient sftpClient = getSFTPClient(origFlowFile);

        try (RemoteFile rf = sftpClient.open(remoteFileName);
             RemoteFile.ReadAheadRemoteFileInputStream rfis = rf.new ReadAheadRemoteFileInputStream(16)) {
            return session.write(origFlowFile, out -> StreamUtils.copy(rfis, out));
        } catch (final SFTPException e) {
            switch (e.getStatusCode()) {
                case NO_SUCH_FILE:
                    throw new FileNotFoundException("Could not find file " + remoteFileName + " on remote SFTP Server");
                case PERMISSION_DENIED:
                    throw new PermissionDeniedException("Insufficient permissions to read file " + remoteFileName + " from remote SFTP Server", e);
                default:
                    throw new IOException("Failed to obtain file content for " + remoteFileName, e);
            }
        }
    }

    @Override
    public void deleteFile(final FlowFile flowFile, final String path, final String remoteFileName) throws IOException {
        final SFTPClient sftpClient = getSFTPClient(flowFile);
        final String fullPath = buildFullPath(path, remoteFileName);
        try {
            sftpClient.rm(fullPath);
        } catch (final SFTPException e) {
            switch (e.getStatusCode()) {
                case NO_SUCH_FILE:
                    throw new FileNotFoundException("Could not find file " + remoteFileName + " to remove from remote SFTP Server");
                case PERMISSION_DENIED:
                    throw new PermissionDeniedException("Insufficient permissions to delete file " + remoteFileName + " from remote SFTP Server", e);
                default:
                    throw new IOException("Failed to delete remote file " + fullPath, e);
            }
        }
    }

    @Override
    public void deleteDirectory(final FlowFile flowFile, final String remoteDirectoryName) throws IOException {
        final SFTPClient sftpClient = getSFTPClient(flowFile);
        try {
            sftpClient.rmdir(remoteDirectoryName);
        } catch (final SFTPException e) {
            throw new IOException("Failed to delete remote directory " + remoteDirectoryName, e);
        }
    }

    @Override
    public void ensureDirectoryExists(final FlowFile flowFile, final File directoryName) throws IOException {
        final SFTPClient sftpClient = getSFTPClient(flowFile);
        final String remoteDirectory = directoryName.getAbsolutePath().replace("\\", "/").replaceAll("^.\\:", "");

        // if we disable the directory listing, we just want to blindly perform the mkdir command,
        // eating failure exceptions thrown (like if the directory already exists).
        if (disableDirectoryListing) {
            try {
                // Blindly create the dir.
                sftpClient.mkdir(remoteDirectory);
                // The remote directory did not exist, and was created successfully.
                return;
            } catch (SFTPException e) {
                if (e.getStatusCode() == Response.StatusCode.NO_SUCH_FILE) {
                    // No Such File. This happens when parent directory was not found.
                    logger.debug("Could not create {} due to 'No such file'. Will try to create the parent dir.", remoteDirectory);
                } else if (e.getStatusCode() == Response.StatusCode.FAILURE) {
                    // Swallow '4: Failure' including the remote directory already exists.
                    logger.debug("Could not blindly create remote directory", e);
                    return;
                } else {
                    throw new IOException("Could not blindly create remote directory due to " + e.getMessage(), e);
                }
            }
        } else {
            try {
                // Check dir existence.
                sftpClient.stat(remoteDirectory);
                // The remote directory already exists.
                return;
            } catch (final SFTPException e) {
                if (e.getStatusCode() != Response.StatusCode.NO_SUCH_FILE) {
                    throw new IOException("Failed to determine if remote directory exists at " + remoteDirectory + " due to " + getMessage(e), e);
                }
            }
        }

        // first ensure parent directories exist before creating this one
        if (directoryName.getParent() != null && !directoryName.getParentFile().equals(new File(File.separator))) {
            ensureDirectoryExists(flowFile, directoryName.getParentFile());
        }
        logger.debug("Remote Directory {} does not exist; creating it", remoteDirectory);
        try {
            sftpClient.mkdir(remoteDirectory);
            logger.debug("Created {}", remoteDirectory);
        } catch (final SFTPException e) {
            throw new IOException("Failed to create remote directory " + remoteDirectory + " due to " + getMessage(e), e);
        }
    }

    private String getMessage(final SFTPException e) {
        if (e.getStatusCode() != null) {
            return e.getStatusCode().getCode() + ": " + e.getMessage();
        } else {
            return e.getMessage();
        }
    }

    protected SFTPClient getSFTPClient(final FlowFile flowFile) throws IOException {
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
        this.sshClient = SSH_CLIENT_PROVIDER.getClient(ctx, attributes);
        this.sftpClient = new SFTPClient(new SFTPEngine(sshClient).init());
        activeHostname = evaledHostname;
        activePort = evaledPort;
        activePassword = evaledPassword;
        activeUsername = evaledUsername;
        activePrivateKeyPath = evaledPrivateKeyPath;
        activePrivateKeyPassphrase = evaledPrivateKeyPassphrase;
        this.closed = false;

        // Configure timeout for sftp operations
        final int dataTimeout = ctx.getProperty(FileTransfer.DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        this.sftpClient.getSFTPEngine().setTimeoutMs(dataTimeout);

        // Attempt to get the home dir
        try {
            this.homeDir = sftpClient.canonicalize("");
        } catch (IOException e) {
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
            if (null != sshClient) {
                sshClient.disconnect();
            }
        } catch (final Exception ex) {
            logger.warn("Failed to close SSHClient", ex);
        }
        sshClient = null;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public FileInfo getRemoteFileInfo(final FlowFile flowFile, final String path, String filename) throws IOException {
        final SFTPClient sftpClient = getSFTPClient(flowFile);

        final String fullPath = buildFullPath(path, filename);
        final FileAttributes fileAttributes;
        try {
            fileAttributes = sftpClient.stat(fullPath);
        } catch (final SFTPException e) {
            if (e.getStatusCode() == Response.StatusCode.NO_SUCH_FILE) {
                return null;
            } else {
                throw new IOException("Failed to obtain file listing for " + path, e);
            }
        }

        if (fileAttributes == null || isDirectory(fileAttributes)) {
            return null;
        } else {
            return newFileInfo(path, filename, fileAttributes);
        }
    }

    @Override
    public String put(final FlowFile flowFile, final String path, final String filename, final InputStream content) throws IOException {
        final SFTPClient sftpClient = getSFTPClient(flowFile);

        // destination path + filename
        final String fullPath = buildFullPath(path, filename);

        // temporary path + filename
        String tempFilename = ctx.getProperty(TEMP_FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        if (tempFilename == null) {
            final boolean dotRename = ctx.getProperty(DOT_RENAME).asBoolean();
            tempFilename = dotRename ? DOT_PREFIX + filename : filename;
        }
        final String tempPath = buildFullPath(path, tempFilename);

        int perms;
        final String permissions = ctx.getProperty(PERMISSIONS).evaluateAttributeExpressions(flowFile).getValue();
        if (permissions == null || permissions.trim().isEmpty()) {
            sftpClient.getFileTransfer().setPreserveAttributes(false); //We will accept whatever the default permissions are of the destination
            perms = 0;
        } else {
            sftpClient.getFileTransfer().setPreserveAttributes(true); //We will use the permissions supplied by evaluating processor property expression
            perms = numberPermissions(permissions);
        }

        try {
            final LocalSourceFile sourceFile = new SFTPFlowFileSourceFile(filename, content, perms);
            sftpClient.put(sourceFile, tempPath);
        } catch (final SFTPException e) {
            throw new IOException("Unable to put content to " + fullPath + " due to " + getMessage(e), e);
        }

        final String lastModifiedTime = ctx.getProperty(LAST_MODIFIED_TIME).evaluateAttributeExpressions(flowFile).getValue();
        if (lastModifiedTime != null && !lastModifiedTime.trim().isEmpty()) {
            try {
                final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                final OffsetDateTime offsetDateTime = OffsetDateTime.parse(lastModifiedTime, dateTimeFormatter);
                int time = (int) offsetDateTime.toEpochSecond();

                final FileAttributes tempAttributes = sftpClient.stat(tempPath);

                final FileAttributes modifiedAttributes = new FileAttributes.Builder()
                        .withAtimeMtime(tempAttributes.getAtime(), time)
                        .build();

                sftpClient.setattr(tempPath, modifiedAttributes);
            } catch (final Exception e) {
                logger.error("Failed to set lastModifiedTime on {} to {} due to {}", tempPath, lastModifiedTime, e);
            }
        }

        final String owner = ctx.getProperty(REMOTE_OWNER).evaluateAttributeExpressions(flowFile).getValue();
        if (owner != null && !owner.trim().isEmpty()) {
            try {
                sftpClient.chown(tempPath, Integer.parseInt(owner));
            } catch (final Exception e) {
                logger.error("Failed to set owner on {} to {} due to {}", tempPath, owner, e);
            }
        }

        final String group = ctx.getProperty(REMOTE_GROUP).evaluateAttributeExpressions(flowFile).getValue();
        if (group != null && !group.trim().isEmpty()) {
            try {
                sftpClient.chgrp(tempPath, Integer.parseInt(group));
            } catch (final Exception e) {
                logger.error("Failed to set group on {} to {} due to {}", tempPath, group, e);
            }
        }

        if (!filename.equals(tempFilename)) {
            try {
                // file was transferred to a temporary filename, attempt to delete destination filename before rename
                sftpClient.rm(fullPath);
            } catch (final SFTPException e) {
                logger.debug("Failed to remove {} before renaming temporary file", fullPath, e);
            }

            try {
                sftpClient.rename(tempPath, fullPath);
            } catch (final SFTPException e) {
                try {
                    sftpClient.rm(tempPath);
                    throw new IOException("Failed to rename dot-file to " + fullPath + " due to " + getMessage(e), e);
                } catch (final SFTPException e1) {
                    throw new IOException("Failed to rename dot-file to " + fullPath + " and failed to delete it when attempting to clean up", e1);
                }
            }
        }

        return fullPath;
    }

    @Override
    public void rename(final FlowFile flowFile, final String source, final String target) throws IOException {
        final SFTPClient sftpClient = getSFTPClient(flowFile);
        try {
            sftpClient.rename(source, target);
        } catch (final SFTPException e) {
            switch (e.getStatusCode()) {
                case NO_SUCH_FILE:
                    throw new FileNotFoundException("No such file or directory");
                case PERMISSION_DENIED:
                    throw new PermissionDeniedException("Could not rename remote file " + source + " to " + target + " due to insufficient permissions");
                default:
                    throw new IOException(e);
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
}
