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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Vector;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.SftpException;

public class SFTPTransfer extends SSHTransfer {

    /**
     * Property which is used to decide if the {@link #ensureDirectoryExists(FlowFile, String)} method should perform a {@link ChannelSftp#ls(String)} before calling
     * {@link ChannelSftp#mkdir(String)}. In most cases, the code should call ls before mkdir, but some weird permission setups (chmod 100) on a directory would cause the 'ls' to throw a permission
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

    private ChannelSftp sftp;
    private String homeDir;

    private final boolean disableDirectoryListing;

    public SFTPTransfer(final ProcessContext processContext, final ComponentLog logger) {
        super(processContext, logger);

        final PropertyValue disableListing = processContext.getProperty(DISABLE_DIRECTORY_LISTING);
        disableDirectoryListing = disableListing == null ? false : Boolean.TRUE.equals(disableListing.asBoolean());
    }

    @Override
    public String getSSHProtocolName() {
        return getProtocolName();
    }

    @Override
    public String getProtocolName() {
        return "sftp";
    }

    @Override
    public List<FileInfo> getListing(final FlowFile flowFile) throws IOException {
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
        getListing(flowFile, path, depth, maxResults, listing);
        return listing;
    }

    @Override
    public List<FileInfo> getDirectoryListing(final FlowFile flowFile, final String remoteFileName) throws IOException {
        final ChannelSftp sftp = getChannel(flowFile);
        final List<FileInfo> listing = new ArrayList<>(1000);

        final Vector<LsEntry> vector;
        try {
            if (remoteFileName == null || remoteFileName.trim().isEmpty()) {
                vector = sftp.ls(".");
            } else {
                vector = sftp.ls(remoteFileName);
            }

            for (final LsEntry entry : vector) {
                final String entryFilename = entry.getFilename();

                if (entryFilename.equals(".") || entryFilename.equals("..")) {
                    continue;
                }

                listing.add(newFileInfo(entry, remoteFileName));
            }
        } catch (final SftpException e) {
            final String pathDesc = remoteFileName == null ? "current directory" : remoteFileName;
            switch (e.id) {
                case ChannelSftp.SSH_FX_NO_SUCH_FILE:
                    throw new FileNotFoundException("Could not perform listing on " + pathDesc + " because could not find the file on the remote server");
                case ChannelSftp.SSH_FX_PERMISSION_DENIED:
                    throw new PermissionDeniedException("Could not perform listing on " + pathDesc + " due to insufficient permissions");
                default:
                    throw new IOException("Failed to obtain file listing for " + pathDesc, e);
            }
        }

        return listing;
    }

    private void getListing(final FlowFile flowFile, final String path, final int depth, final int maxResults, final List<FileInfo> listing) throws IOException {
        if (maxResults < 1 || listing.size() >= maxResults) {
            return;
        }

        if (depth >= 100) {
            logger.warn(this + " had to stop recursively searching directories at a recursive depth of " + depth + " to avoid memory issues");
            return;
        }

        // check if this directory path matches the PATH_FILTER_REGEX
        boolean pathFilterMatches = FileTransfer.isPathMatch(ctx, flowFile, path);

        final boolean recurse = ctx.getProperty(FileTransfer.RECURSIVE_SEARCH).asBoolean();
        final List<FileInfo> subDirs = new ArrayList<>();

        //create FileInfo filter
        FileInfoFilter fileInfoFilter = FileTransfer.createFileInfoFilter(ctx, flowFile);

        try {
            final List<FileInfo> files = getDirectoryListing(flowFile, path);

            //filter files based on rules, get list of sub directories for recursion
            for (FileInfo f:files) {
                if(!fileInfoFilter.accept(null,f)){
                    continue;
                }

                // if is a directory and we're supposed to recurse
                if (recurse && f.isDirectory()) {
                    subDirs.add(f);
                    continue;
                }

                boolean isLink = f.getAttributes().containsKey("IsLink")?Boolean.parseBoolean(f.getAttributes().get("IsLink")):false;

                // if is not a directory and is not a link and it matches
                if(!f.isDirectory() && !isLink && pathFilterMatches){
                    listing.add(f);
                }

                if (listing.size() >= maxResults) {
                    break;
                }
            }
        } catch (final IOException e) {
            throw e;
        }

        for (final FileInfo entry : subDirs) {
            final String entryFilename = entry.getFileName();
            final File newFullPath = new File(path, entryFilename);
            final String newFullForwardPath = newFullPath.getPath().replace("\\", "/");

            try {
                getListing(flowFile, newFullForwardPath, depth + 1, maxResults, listing);
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

        Map<String,String> attributes = new HashMap<>();
        attributes.put("IsLink", Boolean.toString(entry.getAttrs().isLink()));

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
    public void ensureDirectoryExists(final FlowFile flowFile, final String remoteDirectory) throws IOException {
        final File directoryName = new File(remoteDirectory);
        final ChannelSftp channel = getChannel(flowFile);

        // if we disable the directory listing, we just want to blindly perform the mkdir command,
        // eating failure exceptions thrown (like if the directory already exists).
        if (disableDirectoryListing) {
            try {
                // Blindly create the dir.
                channel.mkdir(remoteDirectory);
                // The remote directory did not exist, and was created successfully.
                return;
            } catch (SftpException e) {
                if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                    // No Such File. This happens when parent directory was not found.
                    logger.debug(String.format("Could not create %s due to 'No such file'. Will try to create the parent dir.", remoteDirectory));
                } else if (e.id == ChannelSftp.SSH_FX_FAILURE) {
                    // Swallow '4: Failure' including the remote directory already exists.
                    logger.debug("Could not blindly create remote directory due to " + e.getMessage(), e);
                    return;
                } else {
                    throw new IOException("Could not blindly create remote directory due to " + e.getMessage(), e);
                }
            }
        } else {
            try {
                // Check dir existence.
                channel.stat(remoteDirectory);
                // The remote directory already exists.
                return;
            } catch (final SftpException e) {
                if (e.id != ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                    throw new IOException("Failed to determine if remote directory exists at " + remoteDirectory + " due to " + e, e);
                }
            }
        }

        // first ensure parent directories exist before creating this one
        if (directoryName.getParent() != null && !directoryName.getParentFile().equals(new File(File.separator))) {
            ensureDirectoryExists(flowFile, directoryName.getParentFile().getAbsolutePath());
        }
        logger.debug("Remote Directory {} does not exist; creating it", new Object[] {remoteDirectory});
        try {
            channel.mkdir(remoteDirectory);
            logger.debug("Created {}", new Object[] {remoteDirectory});
        } catch (final SftpException e) {
            throw new IOException("Failed to create remote directory " + remoteDirectory + " due to " + e, e);
        }
    }

    protected ChannelSftp getChannel(final FlowFile flowFile) throws IOException {
        sftp = (ChannelSftp) super.getChannel(flowFile);

        try {
            this.homeDir = sftp.getHome();
        } catch (SftpException e) {
            // For some combination of server configuration and user home directory, getHome() can fail with "2: File not found"
            // Since  homeDir is only used tor SEND provenance event transit uri, this is harmless. Log and continue.
            final String username = ctx.getProperty(USERNAME).evaluateAttributeExpressions(flowFile).getValue();
            logger.debug("Failed to retrieve {} home directory due to {}", new Object[]{username, e.getMessage()});
        }
        return sftp;
    }

    @Override
    public String getHomeDirectory(final FlowFile flowFile) throws IOException {
        getChannel(flowFile);
        return this.homeDir;
    }

    @Override
    @SuppressWarnings("unchecked")
    public FileInfo getRemoteFileInfo(final FlowFile flowFile, String path, String filename) throws IOException {
        final ChannelSftp sftp = getChannel(flowFile);

        if (path == null) {
            path = "/";

            int slashpos = filename.lastIndexOf('/');
            if (slashpos >= 0 && !filename.endsWith("/")) {
                filename = filename.substring(slashpos + 1);
            }
        }

        final String fullPath = path + "/" + filename;

        final Vector<LsEntry> vector;
        try {
            vector = sftp.ls(path);
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
                    throw new FileNotFoundException("No such file or directory");
                case ChannelSftp.SSH_FX_PERMISSION_DENIED:
                    throw new PermissionDeniedException("Could not rename remote file " + source + " to " + target + " due to insufficient permissions");
                default:
                    throw new IOException(e);
            }
        }
    }


}
