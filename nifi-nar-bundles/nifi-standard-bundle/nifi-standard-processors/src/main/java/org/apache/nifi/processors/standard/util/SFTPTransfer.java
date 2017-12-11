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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SFTPTransfer implements FileTransfer {

    public static final PropertyDescriptor PRIVATE_KEY_PATH = new PropertyDescriptor.Builder()
        .name("Private Key Path")
        .description("The fully qualified path to the Private Key file")
        .required(false)
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor PRIVATE_KEY_PASSPHRASE = new PropertyDescriptor.Builder()
        .name("Private Key Passphrase")
        .description("Password for the private key")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .sensitive(true)
        .build();
    public static final PropertyDescriptor HOST_KEY_FILE = new PropertyDescriptor.Builder()
        .name("Host Key File")
        .description("If supplied, the given file will be used as the Host Key; otherwise, no use host key file will be used")
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
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
        .expressionLanguageSupported(true)
        .required(true)
        .defaultValue("22")
        .build();
    public static final PropertyDescriptor USE_KEEPALIVE_ON_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Send Keep Alive On Timeout")
        .description("Indicates whether or not to send a single Keep Alive message when SSH socket times out")
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();

    /**
     * Dynamic property which is used to decide if the {@link #ensureDirectoryExists(FlowFile, File)} method should perform a {@link ChannelSftp#ls(String)} before calling
     * {@link ChannelSftp#mkdir(String)}. In most cases, the code should call ls before mkdir, but some weird permission setups (chmod 100) on a directory would cause the 'ls' to throw a permission
     * exception.
     * <p>
     * This property is dynamic until deemed a worthy inclusion as proper.
     */
    public static final PropertyDescriptor DISABLE_DIRECTORY_LISTING = new PropertyDescriptor.Builder()
        .name("Disable Directory Listing")
        .description("Disables directory listings before operations which might fail, such as configurations which create directory structures.")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .dynamic(true)
        .defaultValue("false")
        .build();

    private final ComponentLog logger;

    private final ProcessContext ctx;
    private Session session;
    private ChannelSftp sftp;
    private boolean closed = false;
    private String homeDir;

    private final boolean disableDirectoryListing;

    public SFTPTransfer(final ProcessContext processContext, final ComponentLog logger) {
        this.ctx = processContext;
        this.logger = logger;

        final PropertyValue disableListing = processContext.getProperty(DISABLE_DIRECTORY_LISTING);
        disableDirectoryListing = disableListing == null ? false : Boolean.TRUE.equals(disableListing.asBoolean());
    }

    @Override
    public String getProtocolName() {
        return "sftp";
    }

    @Override
    public List<FileInfo> getListing() throws IOException {
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
        getListing(path, depth, maxResults, listing);
        return listing;
    }

    private void getListing(final String path, final int depth, final int maxResults, final List<FileInfo> listing) throws IOException {
        if (maxResults < 1 || listing.size() >= maxResults) {
            return;
        }

        if (depth >= 100) {
            logger.warn(this + " had to stop recursively searching directories at a recursive depth of " + depth + " to avoid memory issues");
            return;
        }

        final boolean ignoreDottedFiles = ctx.getProperty(FileTransfer.IGNORE_DOTTED_FILES).asBoolean();
        final boolean recurse = ctx.getProperty(FileTransfer.RECURSIVE_SEARCH).asBoolean();
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

        final ChannelSftp sftp = getChannel(null);
        final boolean isPathMatch = pathFilterMatches;

        final List<LsEntry> subDirs = new ArrayList<>();
        try {
            final LsEntrySelector filter = new LsEntrySelector() {
                @Override
                public int select(final LsEntry entry) {
                    final String entryFilename = entry.getFilename();

                    // skip over 'this directory' and 'parent directory' special
                    // files regardless of ignoring dot files
                    if (entryFilename.equals(".") || entryFilename.equals("..")) {
                        return LsEntrySelector.CONTINUE;
                    }

                    // skip files and directories that begin with a dot if we're
                    // ignoring them
                    if (ignoreDottedFiles && entryFilename.startsWith(".")) {
                        return LsEntrySelector.CONTINUE;
                    }

                    // if is a directory and we're supposed to recurse
                    if (recurse && entry.getAttrs().isDir()) {
                        subDirs.add(entry);
                        return LsEntrySelector.CONTINUE;
                    }

                    // if is not a directory and is not a link and it matches
                    // FILE_FILTER_REGEX - then let's add it
                    if (!entry.getAttrs().isDir() && !entry.getAttrs().isLink() && isPathMatch) {
                        if (pattern == null || pattern.matcher(entryFilename).matches()) {
                            listing.add(newFileInfo(entry, path));
                        }
                    }

                    if (listing.size() >= maxResults) {
                        return LsEntrySelector.BREAK;
                    }

                    return LsEntrySelector.CONTINUE;
                }

            };

            if (path == null || path.trim().isEmpty()) {
                sftp.ls(".", filter);
            } else {
                sftp.ls(path, filter);
            }
        } catch (final SftpException e) {
            final String pathDesc = path == null ? "current directory" : path;
            switch (e.id) {
                case ChannelSftp.SSH_FX_NO_SUCH_FILE:
                    throw new FileNotFoundException("Could not perform listing on " + pathDesc + " because could not find the file on the remote server");
                case ChannelSftp.SSH_FX_PERMISSION_DENIED:
                    throw new PermissionDeniedException("Could not perform listing on " + pathDesc + " due to insufficient permissions");
                default:
                    throw new IOException("Failed to obtain file listing for " + pathDesc, e);
            }
        }

        for (final LsEntry entry : subDirs) {
            final String entryFilename = entry.getFilename();
            final File newFullPath = new File(path, entryFilename);
            final String newFullForwardPath = newFullPath.getPath().replace("\\", "/");

            try {
                getListing(newFullForwardPath, depth + 1, maxResults, listing);
            } catch (final IOException e) {
                logger.error("Unable to get listing from " + newFullForwardPath + "; skipping this subdirectory", e);
            }
        }
    }

    private FileInfo newFileInfo(final LsEntry entry, String path) {
        if (entry == null) {
            return null;
        }
        final File newFullPath = new File(path, entry.getFilename());
        final String newFullForwardPath = newFullPath.getPath().replace("\\", "/");

        String perms = entry.getAttrs().getPermissionsString();
        if (perms.length() > 9) {
            perms = perms.substring(perms.length() - 9);
        }

        FileInfo.Builder builder = new FileInfo.Builder()
            .filename(entry.getFilename())
            .fullPathFileName(newFullForwardPath)
            .directory(entry.getAttrs().isDir())
            .size(entry.getAttrs().getSize())
            .lastModifiedTime(entry.getAttrs().getMTime() * 1000L)
            .permissions(perms)
            .owner(Integer.toString(entry.getAttrs().getUId()))
            .group(Integer.toString(entry.getAttrs().getGId()));
        return builder.build();
    }

    @Override
    public InputStream getInputStream(final String remoteFileName) throws IOException {
        return getInputStream(remoteFileName, null);
    }

    @Override
    public InputStream getInputStream(final String remoteFileName, final FlowFile flowFile) throws IOException {
        final ChannelSftp sftp = getChannel(flowFile);
        try {
            return sftp.get(remoteFileName);
        } catch (final SftpException e) {
            switch (e.id) {
                case ChannelSftp.SSH_FX_NO_SUCH_FILE:
                    throw new FileNotFoundException("Could not find file " + remoteFileName + " on remote SFTP Server");
                case ChannelSftp.SSH_FX_PERMISSION_DENIED:
                    throw new PermissionDeniedException("Insufficient permissions to read file " + remoteFileName + " from remote SFTP Server", e);
                default:
                    throw new IOException("Failed to obtain file content for " + remoteFileName, e);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        // nothing needed here
    }

    @Override
    public boolean flush(final FlowFile flowFile) throws IOException {
        return true;
    }

    @Override
    public void deleteFile(final FlowFile flowFile, final String path, final String remoteFileName) throws IOException {
        final String fullPath = (path == null) ? remoteFileName : (path.endsWith("/")) ? path + remoteFileName : path + "/" + remoteFileName;
        try {
            sftp.rm(fullPath);
        } catch (final SftpException e) {
            switch (e.id) {
                case ChannelSftp.SSH_FX_NO_SUCH_FILE:
                    throw new FileNotFoundException("Could not find file " + remoteFileName + " to remove from remote SFTP Server");
                case ChannelSftp.SSH_FX_PERMISSION_DENIED:
                    throw new PermissionDeniedException("Insufficient permissions to delete file " + remoteFileName + " from remote SFTP Server", e);
                default:
                    throw new IOException("Failed to delete remote file " + fullPath, e);
            }
        }
    }

    @Override
    public void deleteDirectory(final FlowFile flowFile, final String remoteDirectoryName) throws IOException {
        try {
            sftp.rm(remoteDirectoryName);
        } catch (final SftpException e) {
            throw new IOException("Failed to delete remote directory " + remoteDirectoryName, e);
        }
    }

    @Override
    public void ensureDirectoryExists(final FlowFile flowFile, final File directoryName) throws IOException {
        final ChannelSftp channel = getChannel(flowFile);
        final String remoteDirectory = directoryName.getAbsolutePath().replace("\\", "/").replaceAll("^.\\:", "");

        // if we disable the directory listing, we just want to blindly perform the mkdir command,
        // eating any exceptions thrown (like if the directory already exists).
        if (disableDirectoryListing) {
            try {
                channel.mkdir(remoteDirectory);
            } catch (SftpException e) {
                if (e.id != ChannelSftp.SSH_FX_FAILURE) {
                    throw new IOException("Could not blindly create remote directory due to " + e.getMessage(), e);
                }
            }
            return;
        }
        // end if disableDirectoryListing

        boolean exists;
        try {
            channel.stat(remoteDirectory);
            exists = true;
        } catch (final SftpException e) {
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                // No Such File
                exists = false;
            } else {
                throw new IOException("Failed to determine if remote directory exists at " + remoteDirectory + " due to " + e, e);
            }
        }

        if (!exists) {
            // first ensure parent directories exist before creating this one
            if (directoryName.getParent() != null && !directoryName.getParentFile().equals(new File(File.separator))) {
                ensureDirectoryExists(flowFile, directoryName.getParentFile());
            }
            logger.debug("Remote Directory {} does not exist; creating it", new Object[] {remoteDirectory});
            try {
                channel.mkdir(remoteDirectory);
                logger.debug("Created {}", new Object[] {remoteDirectory});
            } catch (final SftpException e) {
                throw new IOException("Failed to create remote directory " + remoteDirectory + " due to " + e, e);
            }
        }
    }

    private ChannelSftp getChannel(final FlowFile flowFile) throws IOException {
        if (sftp != null) {
            String sessionhost = session.getHost();
            String desthost = ctx.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
            if (sessionhost.equals(desthost)) {
                // destination matches so we can keep our current session
                return sftp;
            } else {
                // this flowFile is going to a different destination, reset session
                close();
            }
        }

        final JSch jsch = new JSch();
        try {
            final String username = ctx.getProperty(USERNAME).evaluateAttributeExpressions(flowFile).getValue();
            final Session session = jsch.getSession(username,
                ctx.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue(),
                ctx.getProperty(PORT).evaluateAttributeExpressions(flowFile).asInteger().intValue());

            final String hostKeyVal = ctx.getProperty(HOST_KEY_FILE).getValue();
            if (hostKeyVal != null) {
                jsch.setKnownHosts(hostKeyVal);
            }

            final Properties properties = new Properties();
            properties.setProperty("StrictHostKeyChecking", ctx.getProperty(STRICT_HOST_KEY_CHECKING).asBoolean() ? "yes" : "no");
            properties.setProperty("PreferredAuthentications", "publickey,password,keyboard-interactive");

            final PropertyValue compressionValue = ctx.getProperty(FileTransfer.USE_COMPRESSION);
            if (compressionValue != null && "true".equalsIgnoreCase(compressionValue.getValue())) {
                properties.setProperty("compression.s2c", "zlib@openssh.com,zlib,none");
                properties.setProperty("compression.c2s", "zlib@openssh.com,zlib,none");
            } else {
                properties.setProperty("compression.s2c", "none");
                properties.setProperty("compression.c2s", "none");
            }

            session.setConfig(properties);

            final String privateKeyFile = ctx.getProperty(PRIVATE_KEY_PATH).evaluateAttributeExpressions(flowFile).getValue();
            if (privateKeyFile != null) {
                jsch.addIdentity(privateKeyFile, ctx.getProperty(PRIVATE_KEY_PASSPHRASE).evaluateAttributeExpressions(flowFile).getValue());
            }

            final String password = ctx.getProperty(FileTransfer.PASSWORD).evaluateAttributeExpressions(flowFile).getValue();
            if (password != null) {
                session.setPassword(password);
            }

            final int connectionTimeoutMillis = ctx.getProperty(FileTransfer.CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            session.setTimeout(connectionTimeoutMillis);
            session.connect();
            this.session = session;
            this.closed = false;

            sftp = (ChannelSftp) session.openChannel("sftp");
            sftp.connect(connectionTimeoutMillis);
            session.setTimeout(ctx.getProperty(FileTransfer.DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
            if (!ctx.getProperty(USE_KEEPALIVE_ON_TIMEOUT).asBoolean()) {
                session.setServerAliveCountMax(0); // do not send keepalive message on SocketTimeoutException
            }
            try {
                this.homeDir = sftp.getHome();
            } catch (SftpException e) {
                // For some combination of server configuration and user home directory, getHome() can fail with "2: File not found"
                // Since  homeDir is only used tor SEND provenance event transit uri, this is harmless. Log and continue.
                logger.debug("Failed to retrieve {} home directory due to {}", new Object[]{username, e.getMessage()});
            }
            return sftp;

        } catch (JSchException e) {
            throw new IOException("Failed to obtain connection to remote host due to " + e.toString(), e);
        }
    }

    @Override
    public String getHomeDirectory(final FlowFile flowFile) throws IOException {
        getChannel(flowFile);
        return this.homeDir;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        try {
            if (null != sftp) {
                sftp.exit();
            }
        } catch (final Exception ex) {
            logger.warn("Failed to close ChannelSftp due to {}", new Object[] {ex.toString()}, ex);
        }
        sftp = null;

        try {
            if (null != session) {
                session.disconnect();
            }
        } catch (final Exception ex) {
            logger.warn("Failed to close session due to {}", new Object[] {ex.toString()}, ex);
        }
        session = null;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    @SuppressWarnings("unchecked")
    public FileInfo getRemoteFileInfo(final FlowFile flowFile, final String path, String filename) throws IOException {
        final ChannelSftp sftp = getChannel(flowFile);
        final String fullPath;

        if (path == null) {
            fullPath = filename;
            int slashpos = filename.lastIndexOf('/');
            if (slashpos >= 0 && !filename.endsWith("/")) {
                filename = filename.substring(slashpos + 1);
            }
        } else {
            fullPath = path + "/" + filename;
        }

        final Vector<LsEntry> vector;
        try {
            vector = sftp.ls(fullPath);
        } catch (final SftpException e) {
            // ls throws exception if filename is not present
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                return null;
            } else {
                throw new IOException("Failed to obtain file listing for " + fullPath, e);
            }
        }

        LsEntry matchingEntry = null;
        for (final LsEntry entry : vector) {
            if (entry.getFilename().equalsIgnoreCase(filename)) {
                matchingEntry = entry;
                break;
            }
        }

        return newFileInfo(matchingEntry, path);
    }

    @Override
    public String put(final FlowFile flowFile, final String path, final String filename, final InputStream content) throws IOException {
        final ChannelSftp sftp = getChannel(flowFile);

        // destination path + filename
        final String fullPath = (path == null) ? filename : (path.endsWith("/")) ? path + filename : path + "/" + filename;

        // temporary path + filename
        String tempFilename = ctx.getProperty(TEMP_FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        if (tempFilename == null) {
            final boolean dotRename = ctx.getProperty(DOT_RENAME).asBoolean();
            tempFilename = dotRename ? "." + filename : filename;
        }
        final String tempPath = (path == null) ? tempFilename : (path.endsWith("/")) ? path + tempFilename : path + "/" + tempFilename;

        try {
            sftp.put(content, tempPath);
        } catch (final SftpException e) {
            throw new IOException("Unable to put content to " + fullPath + " due to " + e, e);
        }

        final String lastModifiedTime = ctx.getProperty(LAST_MODIFIED_TIME).evaluateAttributeExpressions(flowFile).getValue();
        if (lastModifiedTime != null && !lastModifiedTime.trim().isEmpty()) {
            try {
                final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                final Date fileModifyTime = formatter.parse(lastModifiedTime);
                int time = (int) (fileModifyTime.getTime() / 1000L);
                sftp.setMtime(tempPath, time);
            } catch (final Exception e) {
                logger.error("Failed to set lastModifiedTime on {} to {} due to {}", new Object[] {tempPath, lastModifiedTime, e});
            }
        }

        final String permissions = ctx.getProperty(PERMISSIONS).evaluateAttributeExpressions(flowFile).getValue();
        if (permissions != null && !permissions.trim().isEmpty()) {
            try {
                int perms = numberPermissions(permissions);
                if (perms >= 0) {
                    sftp.chmod(perms, tempPath);
                }
            } catch (final Exception e) {
                logger.error("Failed to set permission on {} to {} due to {}", new Object[] {tempPath, permissions, e});
            }
        }

        final String owner = ctx.getProperty(REMOTE_OWNER).evaluateAttributeExpressions(flowFile).getValue();
        if (owner != null && !owner.trim().isEmpty()) {
            try {
                sftp.chown(Integer.parseInt(owner), tempPath);
            } catch (final Exception e) {
                logger.error("Failed to set owner on {} to {} due to {}", new Object[] {tempPath, owner, e});
            }
        }

        final String group = ctx.getProperty(REMOTE_GROUP).evaluateAttributeExpressions(flowFile).getValue();
        if (group != null && !group.trim().isEmpty()) {
            try {
                sftp.chgrp(Integer.parseInt(group), tempPath);
            } catch (final Exception e) {
                logger.error("Failed to set group on {} to {} due to {}", new Object[] {tempPath, group, e});
            }
        }

        if (!filename.equals(tempFilename)) {
            try {
                sftp.rename(tempPath, fullPath);
            } catch (final SftpException e) {
                try {
                    sftp.rm(tempPath);
                    throw new IOException("Failed to rename dot-file to " + fullPath + " due to " + e, e);
                } catch (final SftpException e1) {
                    throw new IOException("Failed to rename dot-file to " + fullPath + " and failed to delete it when attempting to clean up", e1);
                }
            }
        }

        return fullPath;
    }

    @Override
    public void rename(final FlowFile flowFile, final String source, final String target) throws IOException {
        final ChannelSftp sftp = getChannel(flowFile);
        try {
            sftp.rename(source, target);
        } catch (final SftpException e) {
            switch (e.id) {
                case ChannelSftp.SSH_FX_NO_SUCH_FILE:
                    throw new FileNotFoundException();
                case ChannelSftp.SSH_FX_PERMISSION_DENIED:
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
            } catch (NumberFormatException ignore) {
            }
        }
        return number;
    }

    static {
        JSch.setLogger(new com.jcraft.jsch.Logger() {
            @Override
            public boolean isEnabled(int level) {
                return true;
            }

            @Override
            public void log(int level, String message) {
                LoggerFactory.getLogger(SFTPTransfer.class).debug("SFTP Log: {}", message);
            }
        });
    }
}
