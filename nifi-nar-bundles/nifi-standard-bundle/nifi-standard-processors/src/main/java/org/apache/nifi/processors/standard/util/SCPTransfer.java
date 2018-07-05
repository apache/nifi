package org.apache.nifi.processors.standard.util;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.Session;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

public class SCPTransfer extends SSHTransfer {
    private static final int DEFAULT_FILE_MODE = 0700;
    private static final int BUFFER_SIZE = 100 * 1024;
    private static final byte LINE_FEED = 0x0a;

    private final DateFormat informat = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);

    private ChannelExec execChannel;
    protected final ByteArrayOutputStream errout = new ByteArrayOutputStream();

    public SCPTransfer(final ProcessContext processContext, final ComponentLog logger) {
        super(processContext, logger);

    }

    @Override
    public String getSSHProtocolName() {
        return "exec";
    }

    @Override
    public String getHomeDirectory(FlowFile flowFile) throws IOException {
        //output from console will have line return after path.
        return executeCommand(flowFile, "pwd").trim();
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
        String searchPath = remoteFileName;
        if (remoteFileName == null || remoteFileName.trim().isEmpty()) {
            searchPath = ".";
        }

        //A, don't print . and .. entries
        //l, long list
        //t, sort by modification time
        //1, print one file per line; there are very few limits on filenames, which can include line returns
        //n, output group/user information as numbers rather than names
        //p, puts a slash after directory names so we can identify them
        //--time-style, used to get time format in parsable way
        String listing = executeCommand(flowFile, "ls -Alt1np --time-style=full-iso \"" + searchPath + "\"");
        String[] fileListings = listing.split("\\n");

        List<FileInfo> files = new ArrayList<>();

        if(fileListings.length > 1){
            //start with 1 to skip Totals row from `ls` command
            for (int i = 1; i < fileListings.length; i++) {
                final String fileString = fileListings[i];
                final String[] fileListParts = fileString.split("\\s+");

                final String fileDate = fileListParts[5];
                final String fileTime = fileListParts[6];
                //add colon to time zone offset for parsing
                final String fileTimeZoneOffset = fileListParts[7].substring(0,3) + ":" + fileListParts[7].substring(3);

                final String fileDateTimeString = fileDate + "T" + fileTime + fileTimeZoneOffset;

                final String filenameRaw = fileListParts[8];
                final Boolean isDirectory = filenameRaw.endsWith("/");
                final String filename = isDirectory?filenameRaw.replace("/", ""):filenameRaw;

                if (filename.equals(".") || filename.equals("..")) {
                    continue;
                }

                files.add(new FileInfo.Builder()
                        .directory(isDirectory)
                        .permissions(fileListParts[0])
                        .owner(fileListParts[2])
                        .group(fileListParts[3])
                        .size(Long.parseLong(fileListParts[4]))
                        .lastModifiedTime(OffsetDateTime.parse(fileDateTimeString).toInstant().getEpochSecond())
                        .filename(filename)
                        .fullPathFileName(searchPath.endsWith("/") ? searchPath + filename : searchPath + "/" + filename).build());
            }
        }

        return files;
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

                // if is not a directory and is not a link and it matches
                // FILE_FILTER_REGEX - then let's add it

                //TODO: how to identify if this file is a link? SFTP code has !entry.getAttrs().isLink()
                if(!f.isDirectory() && pathFilterMatches) {
                    listing.add(f);
                }

                if (listing.size() >= maxResults) {
                    break;
                }
            }
        } catch (final IOException e) {
            final String pathDesc = path == null ? "current directory" : path;
            throw new IOException("Failed to obtain file listing for " + pathDesc, e);
        }

        for (final FileInfo entry : subDirs) {
            try {
                getListing(flowFile, entry.getFullPathFileName(), depth + 1, maxResults, listing);
            } catch (final IOException e) {
                logger.error("Unable to get listing from " + entry.getFullPathFileName() + "; skipping this subdirectory", e);
            }
        }
    }

    @Override
    public InputStream getInputStream(String remoteFileName, FlowFile flowFile) throws IOException {
        ensureChannel(flowFile, "scp -f " + remoteFileName);
        sendAck(out);

        PipedInputStream pipedInputStream = new PipedInputStream();
        OutputStream fos = new PipedOutputStream(pipedInputStream);


        try {
            // C0644 filesize filename - header for a regular file
            // T time 0 time 0\n - present if perserve time.
            // D directory - this is the header for a directory.
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            while (true) {
                final int read = in.read();
                if (read < 0) {
                    return null;
                }
                if ((byte) read == LINE_FEED) {
                    break;
                }
                stream.write(read);
            }

            final String serverResponse = stream.toString("UTF-8");
            if (serverResponse.charAt(0) == 'C') {
                final byte[] buf = new byte[BUFFER_SIZE];
                sendAck(out);

                int start = 0;
                int end = serverResponse.indexOf(' ', start + 1);
                start = end + 1;
                end = serverResponse.indexOf(' ', start + 1);

                long filesize = Long.parseLong(serverResponse.substring(start, end));
                final String filename = serverResponse.substring(end + 1);

                int length;

                try {
                    while (true) {
                        length = in.read(buf, 0,
                                BUFFER_SIZE < filesize ? BUFFER_SIZE
                                        : (int) filesize);
                        if (length < 0) {
                            throw new EOFException("Unexpected end of stream.");
                        }
                        fos.write(buf, 0, length);
                        filesize -= length;
                        if (filesize == 0) {
                            break;
                        }
                    }
                } finally {
                    fos.flush();
                    fos.close();
                }

                waitForAck(in);
                sendAck(out);
            } else if (serverResponse.charAt(0) == '\01'
                    || serverResponse.charAt(0) == '\02') {
                // this indicates an error.
                throw new IOException(serverResponse.substring(1));
            } else {
                throw new IOException("Unknown SCP transfer type " + serverResponse.substring(1));
            }
        } finally {
            close();
        }

        return pipedInputStream;
    }

    @Override
    public FileInfo getRemoteFileInfo(FlowFile flowFile, String path, String remoteFileName) throws IOException {

        return null;
    }

    @Override
    public String put(FlowFile flowFile, String path, String filename, InputStream content) throws IOException {
        //path cleanup
        path = (path == null) ? "" : (path.endsWith("/")) ? path : path + "/";

        // destination path + filename
        final String fullPath = path + filename;

        // temporary path + filename
        String tempFilename = ctx.getProperty(TEMP_FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        if (tempFilename == null) {
            final boolean dotRename = ctx.getProperty(DOT_RENAME).asBoolean();
            tempFilename = dotRename ? "." + filename : filename;
        }

        final String tempFullPath = path + tempFilename;

        ensureChannel(flowFile, "scp -t " + path);
        waitForAck(in);

        final String lastModifiedTime = ctx.getProperty(LAST_MODIFIED_TIME).evaluateAttributeExpressions(flowFile).getValue();
        if (lastModifiedTime != null && !lastModifiedTime.trim().isEmpty()) {
            try {
                Date dtFileModifyTime = informat.parse(lastModifiedTime);
                long fileModifyTime = dtFileModifyTime.getTime();

                String command = "T" + (fileModifyTime / 1000) + " 0";
                command += " " + (fileModifyTime / 1000) + " 0\n";
                out.write(command.getBytes());
                out.flush();

                waitForAck(in);
            } catch (ParseException e) {
                logger.warn("Could not set lastModifiedTime on {} to {}", new Object[] {flowFile, lastModifiedTime});
            }
        }

        String command = "C0";
        final String permissions = ctx.getProperty(PERMISSIONS).evaluateAttributeExpressions(flowFile).getValue();
        if (permissions != null && !permissions.trim().isEmpty()) {
            try {
                int perms = numberPermissions(permissions);
                if (perms == -1) {
                    //File mode is REQUIRED by SCP protcol. Defaulting to 700 if not specified.
                    perms = DEFAULT_FILE_MODE;
                }

                command += Integer.toOctalString(perms);
            } catch (final Exception e) {
                logger.error("Failed to set permission on {} to {} due to {}", new Object[] {path, permissions, e});
            }
        } else {
            command += Integer.toOctalString(DEFAULT_FILE_MODE);
        }

        command += " " + flowFile.getSize() + " ";
        command += tempFilename;
        command += "\n";

        out.write(command.getBytes());
        out.flush();

        waitForAck(in);

        // send a content of file
        final byte[] buf = new byte[BUFFER_SIZE];

        while (true) {
            final int len = content.read(buf, 0, buf.length);
            if (len <= 0) {
                break;
            }
            out.write(buf, 0, len);
        }
        out.flush();
        sendAck(out);
        waitForAck(in);

        if (!filename.equals(tempFilename)) {
            rename(flowFile, tempFullPath, fullPath);
        }

        return fullPath;
    }

    @Override
    public void rename(FlowFile flowFile, String source, String target) throws IOException {
        String command = "mv \"" + source + "\" \"" + target + "\"";

        executeCommand(flowFile, command);
    }

    @Override
    public void deleteFile(FlowFile flowFile, String path, String remoteFileName) throws IOException {
        final String fullPath = (path == null) ? remoteFileName : (path.endsWith("/")) ? path + remoteFileName : path + "/" + remoteFileName;

        String command = "rm \"" + fullPath + "\"";

        executeCommand(flowFile, command);
    }

    @Override
    public void deleteDirectory(FlowFile flowFile, String remoteDirectoryName) throws IOException {
        String command = "rm -r \"" + remoteDirectoryName + "\"";

        executeCommand(flowFile, command);
    }

    @Override
    public String getProtocolName() {
        return "ssh";
    }

    @Override
    public void ensureDirectoryExists(FlowFile flowFile, String remoteDirectory) throws IOException {
        String command = "mkdir -p \"" + remoteDirectory + "\"";

        executeCommand(flowFile, command);
    }

    protected String executeCommand(FlowFile flowFile, String command) throws IOException {
        closeChannel();
        if(!sessionIsValid(flowFile)){
            this.session = SSHTransfer.getSession(ctx, flowFile);
        }

        SSHExec executor = new SSHExec();
        executor.setCommand(command);

        return executor.execute(session);
    }

    protected void ensureChannel(FlowFile flowFile, String command) throws IOException {
        //close existing channel if open.
        //all requests are executed using exec channel, since user may not have permission for shell channel
        closeChannel();

        execChannel = (ChannelExec) getChannel(flowFile, new SetCommandHandler(command));
    }

    /**
     * Reads the response, throws a BuildException if the response
     * indicates an error.
     * @param in the input stream to use
     * @throws IOException on I/O error
     */
    protected void waitForAck(final InputStream in)
            throws IOException, ProcessException {
        final int b = in.read();

        // b may be 0 for success,
        //          1 for error,
        //          2 for fatal error,

        if (b == -1) {
            // didn't receive any response
            throw new ProcessException("No response from server");
        }
        if (b != 0) {
            final StringBuilder sb = new StringBuilder();

            int c = in.read();
            while (c > 0 && c != '\n') {
                sb.append((char) c);
                c = in.read();
            }

            if (b == 1) {
                throw new ProcessException("server indicated an error: "
                        + sb.toString());
            } else if (b == 2) {
                throw new ProcessException("server indicated a fatal error: "
                        + sb.toString());
            } else {
                throw new ProcessException("unknown response, code " + b
                        + " message: " + sb.toString());
            }
        }
    }

    /**
     * Send an ack.
     * @param out the output stream to use
     * @throws IOException on error
     */
    protected void sendAck(final OutputStream out) throws IOException {
        final byte[] buf = new byte[1];
        buf[0] = 0;
        out.write(buf);
        out.flush();
    }


    protected class SetCommandHandler implements PreConnectHandler {
        private final String command;

        public SetCommandHandler(String command){
            this.command = command;
        }

        @Override
        public void OnPreConnect(Channel channel, FlowFile flowFile) throws IOException {
            ChannelExec execChannel = (ChannelExec) channel;

            execChannel.setCommand(this.command);

            execChannel.setErrStream(errout);
        }
    }
}
