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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPHTTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;

public class FTPTransfer implements FileTransfer {

    public static final String CONNECTION_MODE_ACTIVE = "Active";
    public static final String CONNECTION_MODE_PASSIVE = "Passive";
    public static final String TRANSFER_MODE_ASCII = "ASCII";
    public static final String TRANSFER_MODE_BINARY = "Binary";
    public static final String FTP_TIMEVAL_FORMAT = "yyyyMMddHHmmss";
    public static final String PROXY_TYPE_DIRECT = Proxy.Type.DIRECT.name();
    public static final String PROXY_TYPE_HTTP = Proxy.Type.HTTP.name();
    public static final String PROXY_TYPE_SOCKS = Proxy.Type.SOCKS.name();

    public static final PropertyDescriptor CONNECTION_MODE = new PropertyDescriptor.Builder()
        .name("Connection Mode")
        .description("The FTP Connection Mode")
        .allowableValues(CONNECTION_MODE_ACTIVE, CONNECTION_MODE_PASSIVE)
        .defaultValue(CONNECTION_MODE_PASSIVE)
        .build();
    public static final PropertyDescriptor TRANSFER_MODE = new PropertyDescriptor.Builder()
        .name("Transfer Mode")
        .description("The FTP Transfer Mode")
        .allowableValues(TRANSFER_MODE_BINARY, TRANSFER_MODE_ASCII)
        .defaultValue(TRANSFER_MODE_BINARY)
        .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
        .name("Port")
        .description("The port that the remote system is listening on for file transfers")
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .required(true)
        .defaultValue("21")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    public static final PropertyDescriptor PROXY_TYPE = new PropertyDescriptor.Builder()
        .name("Proxy Type")
        .description("Proxy type used for file transfers")
        .allowableValues(PROXY_TYPE_DIRECT, PROXY_TYPE_HTTP, PROXY_TYPE_SOCKS)
        .defaultValue(PROXY_TYPE_DIRECT)
        .build();
    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
        .name("Proxy Host")
        .description("The fully qualified hostname or IP address of the proxy server")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();
    public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor.Builder()
        .name("Proxy Port")
        .description("The port of the proxy server")
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();
    public static final PropertyDescriptor HTTP_PROXY_USERNAME = new PropertyDescriptor.Builder()
        .name("Http Proxy Username")
        .description("Http Proxy Username")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(false)
        .build();
    public static final PropertyDescriptor HTTP_PROXY_PASSWORD = new PropertyDescriptor.Builder()
        .name("Http Proxy Password")
        .description("Http Proxy Password")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(false)
        .sensitive(true)
        .build();
    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
        .name("Internal Buffer Size")
        .description("Set the internal buffer size for buffered data streams")
        .defaultValue("16KB")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .build();
    public static final PropertyDescriptor UTF8_ENCODING = new PropertyDescriptor.Builder()
            .name("ftp-use-utf8")
            .displayName("Use UTF-8 Encoding")
            .description("Tells the client to use UTF-8 encoding when processing files and filenames. If set to true, the server must also support UTF-8 encoding.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH, ProxySpec.SOCKS};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE
            = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);

    private final ComponentLog logger;

    private final ProcessContext ctx;
    private boolean closed = true;
    private FTPClient client;
    private String homeDirectory;
    private String remoteHostName;

    public FTPTransfer(final ProcessContext context, final ComponentLog logger) {
        this.ctx = context;
        this.logger = logger;
    }

    public static void validateProxySpec(ValidationContext context, Collection<ValidationResult> results) {
        ProxyConfiguration.validateProxySpec(context, results, PROXY_SPECS);
    }

    @Override
    public String getProtocolName() {
        return "ftp";
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        try {
            if (null != client) {
                client.disconnect();
            }
        } catch (final Exception ex) {
            logger.warn("Failed to close FTPClient due to {}", new Object[] {ex.toString()}, ex);
        }
        client = null;
    }

    @Override
    public String getHomeDirectory(final FlowFile flowFile) throws IOException {
        getClient(flowFile);
        return homeDirectory;
    }

    @Override
    public List<FileInfo> getListing() throws IOException {
        final String path = ctx.getProperty(FileTransfer.REMOTE_PATH).evaluateAttributeExpressions().getValue();
        final int depth = 0;
        final int maxResults = ctx.getProperty(FileTransfer.REMOTE_POLL_BATCH_SIZE).asInteger();
        return getListing(path, depth, maxResults);
    }

    private List<FileInfo> getListing(final String path, final int depth, final int maxResults) throws IOException {
        final List<FileInfo> listing = new ArrayList<>();
        if (maxResults < 1) {
            return listing;
        }

        if (depth >= 100) {
            logger.warn(this + " had to stop recursively searching directories at a recursive depth of " + depth + " to avoid memory issues");
            return listing;
        }

        final boolean ignoreDottedFiles = ctx.getProperty(FileTransfer.IGNORE_DOTTED_FILES).asBoolean();
        final boolean recurse = ctx.getProperty(FileTransfer.RECURSIVE_SEARCH).asBoolean();
        final boolean symlink = ctx.getProperty(FileTransfer.FOLLOW_SYMLINK).asBoolean();
        final String fileFilterRegex = ctx.getProperty(FileTransfer.FILE_FILTER_REGEX).getValue();
        final Pattern pattern = (fileFilterRegex == null) ? null : Pattern.compile(fileFilterRegex);
        final String pathFilterRegex = ctx.getProperty(FileTransfer.PATH_FILTER_REGEX).getValue();
        final Pattern pathPattern = (!recurse || pathFilterRegex == null) ? null : Pattern.compile(pathFilterRegex);
        final String remotePath = ctx.getProperty(FileTransfer.REMOTE_PATH).evaluateAttributeExpressions().getValue();

        // check if this directory path matches the PATH_FILTER_REGEX
        boolean pathFilterMatches = true;
        if (pathPattern != null) {
            Path reldir = path == null ? Paths.get(".") : Paths.get(path);
            if (remotePath != null) {
                reldir = Paths.get(remotePath).relativize(reldir);
            }
            if (reldir != null && !reldir.toString().isEmpty()) {
                if (!pathPattern.matcher(reldir.toString().replace("\\", "/")).matches()) {
                    pathFilterMatches = false;
                }
            }
        }

        final FTPClient client = getClient(null);

        int count = 0;
        final FTPFile[] files;

        if (path == null || path.trim().isEmpty()) {
            files = client.listFiles(".");
        } else {
            files = client.listFiles(path);
        }
        if (files.length == 0 && path != null && !path.trim().isEmpty()) {
            // throw exception if directory doesn't exist
            final boolean cdSuccessful = setWorkingDirectory(path);
            if (!cdSuccessful) {
                throw new IOException("Cannot list files for non-existent directory " + path);
            }
        }

        for (final FTPFile file : files) {
            final String filename = file.getName();
            if (filename.equals(".") || filename.equals("..")) {
                continue;
            }

            if (ignoreDottedFiles && filename.startsWith(".")) {
                continue;
            }

            final File newFullPath = new File(path, filename);
            final String newFullForwardPath = newFullPath.getPath().replace("\\", "/");

            // if is a directory and we're supposed to recurse
            // OR if is a link and we're supposed to follow symlink
            if ((recurse && file.isDirectory()) || (symlink && file.isSymbolicLink())) {
                try {
                    listing.addAll(getListing(newFullForwardPath, depth + 1, maxResults - count));
                } catch (final IOException e) {
                    logger.error("Unable to get listing from " + newFullForwardPath + "; skipping", e);
                }
            }

            // if is not a directory and is not a link and it matches
            // FILE_FILTER_REGEX - then let's add it
            if (!file.isDirectory() && !file.isSymbolicLink() && pathFilterMatches) {
                if (pattern == null || pattern.matcher(filename).matches()) {
                    listing.add(newFileInfo(file, path));
                    count++;
                }
            }

            if (count >= maxResults) {
                break;
            }
        }

        return listing;
    }

    private FileInfo newFileInfo(final FTPFile file, String path) {
        if (file == null) {
            return null;
        }
        final File newFullPath = new File(path, file.getName());
        final String newFullForwardPath = newFullPath.getPath().replace("\\", "/");
        StringBuilder perms = new StringBuilder();
        perms.append(file.hasPermission(FTPFile.USER_ACCESS, FTPFile.READ_PERMISSION) ? "r" : "-");
        perms.append(file.hasPermission(FTPFile.USER_ACCESS, FTPFile.WRITE_PERMISSION) ? "w" : "-");
        perms.append(file.hasPermission(FTPFile.USER_ACCESS, FTPFile.EXECUTE_PERMISSION) ? "x" : "-");
        perms.append(file.hasPermission(FTPFile.GROUP_ACCESS, FTPFile.READ_PERMISSION) ? "r" : "-");
        perms.append(file.hasPermission(FTPFile.GROUP_ACCESS, FTPFile.WRITE_PERMISSION) ? "w" : "-");
        perms.append(file.hasPermission(FTPFile.GROUP_ACCESS, FTPFile.EXECUTE_PERMISSION) ? "x" : "-");
        perms.append(file.hasPermission(FTPFile.WORLD_ACCESS, FTPFile.READ_PERMISSION) ? "r" : "-");
        perms.append(file.hasPermission(FTPFile.WORLD_ACCESS, FTPFile.WRITE_PERMISSION) ? "w" : "-");
        perms.append(file.hasPermission(FTPFile.WORLD_ACCESS, FTPFile.EXECUTE_PERMISSION) ? "x" : "-");

        FileInfo.Builder builder = new FileInfo.Builder()
            .filename(file.getName())
            .fullPathFileName(newFullForwardPath)
            .directory(file.isDirectory())
            .size(file.getSize())
            .lastModifiedTime(file.getTimestamp().getTimeInMillis())
            .permissions(perms.toString())
            .owner(file.getUser())
            .group(file.getGroup());
        return builder.build();
    }

    @Override
    public InputStream getInputStream(String remoteFileName) throws IOException {
        return getInputStream(remoteFileName, null);
    }

    @Override
    public InputStream getInputStream(final String remoteFileName, final FlowFile flowFile) throws IOException {
        final FTPClient client = getClient(flowFile);
        InputStream in = client.retrieveFileStream(remoteFileName);
        if (in == null) {
            throw new IOException(client.getReplyString());
        }
        return in;
    }

    @Override
    public void flush() throws IOException {
        final FTPClient client = getClient(null);
        client.completePendingCommand();
    }

    @Override
    public boolean flush(final FlowFile flowFile) throws IOException {
        return getClient(flowFile).completePendingCommand();
    }

    @Override
    public FileInfo getRemoteFileInfo(final FlowFile flowFile, String path, String remoteFileName) throws IOException {
        final FTPClient client = getClient(flowFile);

        if (path == null) {
            int slashpos = remoteFileName.lastIndexOf('/');
            if (slashpos >= 0 && !remoteFileName.endsWith("/")) {
                path = remoteFileName.substring(0, slashpos);
                remoteFileName = remoteFileName.substring(slashpos + 1);
            } else {
                path = "";
            }
        }

        final FTPFile[] files = client.listFiles(path);
        FTPFile matchingFile = null;
        for (final FTPFile file : files) {
            if (file.getName().equalsIgnoreCase(remoteFileName)) {
                matchingFile = file;
                break;
            }
        }

        if (matchingFile == null) {
            return null;
        }

        return newFileInfo(matchingFile, path);
    }

    @Override
    public void ensureDirectoryExists(final FlowFile flowFile, final File directoryName) throws IOException {
        if (directoryName.getParent() != null && !directoryName.getParentFile().equals(new File(File.separator))) {
            ensureDirectoryExists(flowFile, directoryName.getParentFile());
        }

        final String remoteDirectory = directoryName.getAbsolutePath().replace("\\", "/").replaceAll("^.\\:", "");
        final FTPClient client = getClient(flowFile);
        final boolean cdSuccessful = setWorkingDirectory(remoteDirectory);

        if (!cdSuccessful) {
            logger.debug("Remote Directory {} does not exist; creating it", new Object[] {remoteDirectory});
            if (client.makeDirectory(remoteDirectory)) {
                logger.debug("Created {}", new Object[] {remoteDirectory});
            } else {
                throw new IOException("Failed to create remote directory " + remoteDirectory);
            }
        }
    }

    private String setAndGetWorkingDirectory(final String path) throws IOException {
        client.changeWorkingDirectory(homeDirectory);
        if (!client.changeWorkingDirectory(path)) {
            throw new ProcessException("Unable to change working directory to " + path);
        }
        return client.printWorkingDirectory();
    }

    private boolean setWorkingDirectory(final String path) throws IOException {
        client.changeWorkingDirectory(homeDirectory);
        return client.changeWorkingDirectory(path);
    }

    private boolean resetWorkingDirectory() throws IOException {
        return client.changeWorkingDirectory(homeDirectory);
    }

    @Override
    public String put(final FlowFile flowFile, final String path, final String filename, final InputStream content) throws IOException {
        final FTPClient client = getClient(flowFile);

        final String fullPath;
        if (path == null) {
            fullPath = filename;
        } else {
            final String workingDir = setAndGetWorkingDirectory(path);
            fullPath = workingDir.endsWith("/") ? workingDir + filename : workingDir + "/" + filename;
        }

        String tempFilename = ctx.getProperty(TEMP_FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        if (tempFilename == null) {
            final boolean dotRename = ctx.getProperty(DOT_RENAME).asBoolean();
            tempFilename = dotRename ? "." + filename : filename;
        }

        final boolean storeSuccessful = client.storeFile(tempFilename, content);
        if (!storeSuccessful) {
            throw new IOException("Failed to store file " + tempFilename + " to " + fullPath + " due to: " + client.getReplyString());
        }

        final String lastModifiedTime = ctx.getProperty(LAST_MODIFIED_TIME).evaluateAttributeExpressions(flowFile).getValue();
        if (lastModifiedTime != null && !lastModifiedTime.trim().isEmpty()) {
            try {
                final DateFormat informat = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                final Date fileModifyTime = informat.parse(lastModifiedTime);
                final DateFormat outformat = new SimpleDateFormat(FTP_TIMEVAL_FORMAT, Locale.US);
                final String time = outformat.format(fileModifyTime);
                if (!client.setModificationTime(tempFilename, time)) {
                    // FTP server probably doesn't support MFMT command
                    logger.warn("Could not set lastModifiedTime on {} to {}", new Object[] {flowFile, lastModifiedTime});
                }
            } catch (final Exception e) {
                logger.error("Failed to set lastModifiedTime on {} to {} due to {}", new Object[] {flowFile, lastModifiedTime, e});
            }
        }
        final String permissions = ctx.getProperty(PERMISSIONS).evaluateAttributeExpressions(flowFile).getValue();
        if (permissions != null && !permissions.trim().isEmpty()) {
            try {
                int perms = numberPermissions(permissions);
                if (perms >= 0) {
                    if (!client.sendSiteCommand("chmod " + Integer.toOctalString(perms) + " " + tempFilename)) {
                        logger.warn("Could not set permission on {} to {}", new Object[] {flowFile, permissions});
                    }
                }
            } catch (final Exception e) {
                logger.error("Failed to set permission on {} to {} due to {}", new Object[] {flowFile, permissions, e});
            }
        }

        if (!filename.equals(tempFilename)) {
            try {
                logger.debug("Renaming remote path from {} to {} for {}", new Object[] {tempFilename, filename, flowFile});
                final boolean renameSuccessful = client.rename(tempFilename, filename);
                if (!renameSuccessful) {
                    throw new IOException("Failed to rename temporary file " + tempFilename + " to " + fullPath + " due to: " + client.getReplyString());
                }
            } catch (final IOException e) {
                try {
                    client.deleteFile(tempFilename);
                    throw e;
                } catch (final IOException e1) {
                    throw new IOException("Failed to rename temporary file " + tempFilename + " to " + fullPath + " and failed to delete it when attempting to clean up", e1);
                }
            }
        }

        return fullPath;
    }


    @Override
    public void rename(final FlowFile flowFile, final String source, final String target) throws IOException {
        final FTPClient client = getClient(flowFile);
        final boolean renameSuccessful = client.rename(source, target);
        if (!renameSuccessful) {
            throw new IOException("Failed to rename temporary file " + source + " to " + target + " due to: " + client.getReplyString());
        }
    }

    @Override
    public void deleteFile(final FlowFile flowFile, final String path, final String remoteFileName) throws IOException {
        final FTPClient client = getClient(flowFile);
        if (path != null) {
            setWorkingDirectory(path);
        }
        if (!client.deleteFile(remoteFileName)) {
            throw new IOException("Failed to remove file " + remoteFileName + " due to " + client.getReplyString());
        }
    }

    @Override
    public void deleteDirectory(final FlowFile flowFile, final String remoteDirectoryName) throws IOException {
        final FTPClient client = getClient(flowFile);
        final boolean success = client.removeDirectory(remoteDirectoryName);
        if (!success) {
            throw new IOException("Failed to remove directory " + remoteDirectoryName + " due to " + client.getReplyString());
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    public void sendCommands(final List<String> commands, final FlowFile flowFile) throws IOException {
        if (commands.isEmpty()) {
            return;
        }

        final FTPClient client = getClient(flowFile);
        for (String cmd : commands) {
            if (!cmd.isEmpty()) {
                int result;
                result = client.sendCommand(cmd);
                logger.debug(this + " sent command to the FTP server: " + cmd + " for " + flowFile);

                if (FTPReply.isNegativePermanent(result) || FTPReply.isNegativeTransient(result)) {
                    throw new IOException(this + " negative reply back from FTP server cmd: " + cmd + " reply:" + result + ": " + client.getReplyString() + " for " + flowFile);
                }
            }
        }
    }

    private FTPClient getClient(final FlowFile flowFile) throws IOException {
        if (client != null) {
            String desthost = ctx.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
            if (remoteHostName.equals(desthost)) {
                // destination matches so we can keep our current session
                resetWorkingDirectory();
                return client;
            } else {
                // this flowFile is going to a different destination, reset session
                close();
            }
        }

        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(ctx, createComponentProxyConfigSupplier(ctx));

        final Proxy.Type proxyType = proxyConfig.getProxyType();
        final String proxyHost = proxyConfig.getProxyServerHost();
        final Integer proxyPort = proxyConfig.getProxyServerPort();

        FTPClient client;
        if (proxyType == Proxy.Type.HTTP) {
            client = new FTPHTTPClient(proxyHost, proxyPort, proxyConfig.getProxyUserName(), proxyConfig.getProxyUserPassword());
        } else {
            client = new FTPClient();
            if (proxyType == Proxy.Type.SOCKS) {
                client.setSocketFactory(new SocksProxySocketFactory(new Proxy(proxyType, new InetSocketAddress(proxyHost, proxyPort))));
            }
        }
        this.client = client;
        client.setBufferSize(ctx.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue());
        client.setDataTimeout(ctx.getProperty(DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
        client.setDefaultTimeout(ctx.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
        client.setRemoteVerificationEnabled(false);

        final String remoteHostname = ctx.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
        this.remoteHostName = remoteHostname;
        InetAddress inetAddress = null;
        try {
            inetAddress = InetAddress.getByAddress(remoteHostname, null);
        } catch (final UnknownHostException uhe) {
        }

        if (inetAddress == null) {
            inetAddress = InetAddress.getByName(remoteHostname);
        }

        final boolean useUtf8Encoding = ctx.getProperty(UTF8_ENCODING).isSet() ? ctx.getProperty(UTF8_ENCODING).asBoolean() : false;
        if (useUtf8Encoding) {
            client.setControlEncoding("UTF-8");
        }

        client.connect(inetAddress, ctx.getProperty(PORT).evaluateAttributeExpressions(flowFile).asInteger());
        this.closed = false;
        client.setDataTimeout(ctx.getProperty(DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
        client.setSoTimeout(ctx.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());

        final String username = ctx.getProperty(USERNAME).evaluateAttributeExpressions(flowFile).getValue();
        final String password = ctx.getProperty(PASSWORD).evaluateAttributeExpressions(flowFile).getValue();
        final boolean loggedIn = client.login(username, password);
        if (!loggedIn) {
            throw new IOException("Could not login for user '" + username + "'");
        }

        final String connectionMode = ctx.getProperty(CONNECTION_MODE).getValue();
        if (connectionMode.equalsIgnoreCase(CONNECTION_MODE_ACTIVE)) {
            client.enterLocalActiveMode();
        } else {
            client.enterLocalPassiveMode();
        }

        final String transferMode = ctx.getProperty(TRANSFER_MODE).getValue();
        final int fileType = (transferMode.equalsIgnoreCase(TRANSFER_MODE_ASCII)) ? FTPClient.ASCII_FILE_TYPE : FTPClient.BINARY_FILE_TYPE;
        if (!client.setFileType(fileType)) {
            throw new IOException("Unable to set transfer mode to type " + transferMode);
        }

        this.homeDirectory = client.printWorkingDirectory();
        return client;
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
            } catch (NumberFormatException ignore) {
            }
        }
        return number;
    }

    public static Supplier<ProxyConfiguration> createComponentProxyConfigSupplier(final PropertyContext ctx) {
        return () -> {
            final ProxyConfiguration componentProxyConfig = new ProxyConfiguration();
            componentProxyConfig.setProxyType(Proxy.Type.valueOf(ctx.getProperty(PROXY_TYPE).getValue()));
            componentProxyConfig.setProxyServerHost(ctx.getProperty(PROXY_HOST).evaluateAttributeExpressions().getValue());
            componentProxyConfig.setProxyServerPort(ctx.getProperty(PROXY_PORT).evaluateAttributeExpressions().asInteger());
            componentProxyConfig.setProxyUserName(ctx.getProperty(HTTP_PROXY_USERNAME).evaluateAttributeExpressions().getValue());
            componentProxyConfig.setProxyUserPassword(ctx.getProperty(HTTP_PROXY_PASSWORD).evaluateAttributeExpressions().getValue());
            return componentProxyConfig;
        };
    }

}
