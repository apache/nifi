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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.util.StandardValidators;

public interface FileTransfer extends Closeable {

    String getHomeDirectory(FlowFile flowFile) throws IOException;

    List<FileInfo> getListing() throws IOException;

    InputStream getInputStream(String remoteFileName) throws IOException;

    InputStream getInputStream(String remoteFileName, FlowFile flowFile) throws IOException;

    void flush() throws IOException;

    boolean flush(FlowFile flowFile) throws IOException;

    FileInfo getRemoteFileInfo(FlowFile flowFile, String path, String remoteFileName) throws IOException;

    String put(FlowFile flowFile, String path, String filename, InputStream content) throws IOException;

    void rename(FlowFile flowFile, String source, String target) throws IOException;

    void deleteFile(FlowFile flowFile, String path, String remoteFileName) throws IOException;

    void deleteDirectory(FlowFile flowFile, String remoteDirectoryName) throws IOException;

    boolean isClosed();

    String getProtocolName();

    void ensureDirectoryExists(FlowFile flowFile, File remoteDirectory) throws IOException;

    /**
     * Compute an absolute file path for the given remote path.
     * @param flowFile is used to setup file transfer client with its attribute values, to get user home directory
     * @param remotePath the target remote path
     * @return The absolute path for the given remote path
     */
    default String getAbsolutePath(FlowFile flowFile, String remotePath) throws IOException {
        final String absoluteRemotePath;
        if (!remotePath.startsWith("/") && !remotePath.startsWith("\\")) {
            absoluteRemotePath = new File(getHomeDirectory(flowFile), remotePath).getPath();
        } else {
            absoluteRemotePath = remotePath;
        }
        return absoluteRemotePath.replace("\\", "/");
    }

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
        .name("Hostname")
        .description("The fully qualified hostname or IP address of the remote system")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("Username")
        .description("Username")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
        .name("Password")
        .description("Password for the user account")
        .addValidator(Validator.VALID)
        .required(false)
        .sensitive(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    public static final PropertyDescriptor DATA_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Data Timeout")
        .description("When transferring a file between the local and remote system, this value specifies how long is allowed to elapse without any data being transferred between systems")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("30 sec")
        .build();
    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Connection Timeout")
        .description("Amount of time to wait before timing out while creating a connection")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("30 sec")
        .build();
    public static final PropertyDescriptor REMOTE_PATH = new PropertyDescriptor.Builder()
        .name("Remote Path")
        .description("The path on the remote system from which to pull or push files")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();
    public static final PropertyDescriptor CREATE_DIRECTORY = new PropertyDescriptor.Builder()
        .name("Create Directory")
        .description("Specifies whether or not the remote directory should be created if it does not exist.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

    public static final PropertyDescriptor USE_COMPRESSION = new PropertyDescriptor.Builder()
        .name("Use Compression")
        .description("Indicates whether or not ZLIB compression should be used when transferring files")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    // GET-specific properties
    public static final PropertyDescriptor RECURSIVE_SEARCH = new PropertyDescriptor.Builder()
        .name("Search Recursively")
        .description("If true, will pull files from arbitrarily nested subdirectories; otherwise, will not traverse subdirectories")
        .required(true)
        .defaultValue("false")
        .allowableValues("true", "false")
        .build();
    public static final PropertyDescriptor FOLLOW_SYMLINK = new PropertyDescriptor.Builder()
        .name("follow-symlink")
        .displayName("Follow symlink")
        .description("If true, will pull even symbolic files and also nested symbolic subdirectories; otherwise, will not read symbolic files and will not traverse symbolic link subdirectories")
        .required(true)
        .defaultValue("false")
        .allowableValues("true", "false")
        .build();
    public static final PropertyDescriptor FILE_FILTER_REGEX = new PropertyDescriptor.Builder()
        .name("File Filter Regex")
        .description("Provides a Java Regular Expression for filtering Filenames; if a filter is supplied, only files whose names match that Regular Expression will be fetched")
        .required(false)
        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
        .build();
    public static final PropertyDescriptor PATH_FILTER_REGEX = new PropertyDescriptor.Builder()
        .name("Path Filter Regex")
        .description("When " + RECURSIVE_SEARCH.getName() + " is true, then only subdirectories whose path matches the given Regular Expression will be scanned")
        .required(false)
        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
        .build();
    public static final PropertyDescriptor MAX_SELECTS = new PropertyDescriptor.Builder()
        .name("Max Selects")
        .description("The maximum number of files to pull in a single connection")
        .defaultValue("100")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();
    public static final PropertyDescriptor REMOTE_POLL_BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Remote Poll Batch Size")
        .description("The value specifies how many file paths to find in a given directory on the remote system when doing a file listing. This value "
            + "in general should not need to be modified but when polling against a remote system with a tremendous number of files this value can "
            + "be critical.  Setting this value too high can result very poor performance and setting it too low can cause the flow to be slower "
            + "than normal.")
        .defaultValue("5000")
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .required(true)
        .build();
    public static final PropertyDescriptor DELETE_ORIGINAL = new PropertyDescriptor.Builder()
        .name("Delete Original")
        .description("Determines whether or not the file is deleted from the remote system after it has been successfully transferred")
        .defaultValue("true")
        .allowableValues("true", "false")
        .required(true)
        .build();
    public static final PropertyDescriptor POLLING_INTERVAL = new PropertyDescriptor.Builder()
        .name("Polling Interval")
        .description("Determines how long to wait between fetching the listing for new files")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .required(true)
        .defaultValue("60 sec")
        .build();
    public static final PropertyDescriptor IGNORE_DOTTED_FILES = new PropertyDescriptor.Builder()
        .name("Ignore Dotted Files")
        .description("If true, files whose names begin with a dot (\".\") will be ignored")
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();
    public static final PropertyDescriptor USE_NATURAL_ORDERING = new PropertyDescriptor.Builder()
        .name("Use Natural Ordering")
        .description("If true, will pull files in the order in which they are naturally listed; otherwise, the order in which the files will be pulled is not defined")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    // PUT-specific properties
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    public static final String CONFLICT_RESOLUTION_REPLACE = "REPLACE";
    public static final String CONFLICT_RESOLUTION_RENAME = "RENAME";
    public static final String CONFLICT_RESOLUTION_IGNORE = "IGNORE";
    public static final String CONFLICT_RESOLUTION_REJECT = "REJECT";
    public static final String CONFLICT_RESOLUTION_FAIL = "FAIL";
    public static final String CONFLICT_RESOLUTION_NONE = "NONE";
    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
        .name("Conflict Resolution")
        .description("Determines how to handle the problem of filename collisions")
        .required(true)
        .allowableValues(CONFLICT_RESOLUTION_REPLACE, CONFLICT_RESOLUTION_IGNORE, CONFLICT_RESOLUTION_RENAME, CONFLICT_RESOLUTION_REJECT, CONFLICT_RESOLUTION_FAIL, CONFLICT_RESOLUTION_NONE)
        .defaultValue(CONFLICT_RESOLUTION_NONE)
        .build();
    public static final PropertyDescriptor REJECT_ZERO_BYTE = new PropertyDescriptor.Builder()
        .name("Reject Zero-Byte Files")
        .description("Determines whether or not Zero-byte files should be rejected without attempting to transfer")
        .allowableValues("true", "false")
        .defaultValue("true")
        .build();
    public static final PropertyDescriptor DOT_RENAME = new PropertyDescriptor.Builder()
        .name("Dot Rename")
        .description("If true, then the filename of the sent file is prepended with a \".\" and then renamed back to the "
            + "original once the file is completely sent. Otherwise, there is no rename. This property is ignored if the "
            + "Temporary Filename property is set.")
        .allowableValues("true", "false")
        .defaultValue("true")
        .build();
    public static final PropertyDescriptor TEMP_FILENAME = new PropertyDescriptor.Builder()
        .name("Temporary Filename")
        .description("If set, the filename of the sent file will be equal to the value specified during the transfer and after successful "
            + "completion will be renamed to the original filename. If this value is set, the Dot Rename property is ignored.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .build();
    public static final PropertyDescriptor LAST_MODIFIED_TIME = new PropertyDescriptor.Builder()
        .name("Last Modified Time")
        .description("The lastModifiedTime to assign to the file after transferring it. If not set, the lastModifiedTime will not be changed. "
            + "Format must be yyyy-MM-dd'T'HH:mm:ssZ. You may also use expression language such as ${file.lastModifiedTime}. If the value "
            + "is invalid, the processor will not be invalid but will fail to change lastModifiedTime of the file.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    public static final PropertyDescriptor PERMISSIONS = new PropertyDescriptor.Builder()
        .name("Permissions")
        .description("The permissions to assign to the file after transferring it. Format must be either UNIX rwxrwxrwx with a - in place of "
            + "denied permissions (e.g. rw-r--r--) or an octal number (e.g. 644). If not set, the permissions will not be changed. You may "
            + "also use expression language such as ${file.permissions}. If the value is invalid, the processor will not be invalid but will "
            + "fail to change permissions of the file.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    public static final PropertyDescriptor REMOTE_OWNER = new PropertyDescriptor.Builder()
        .name("Remote Owner")
        .description("Integer value representing the User ID to set on the file after transferring it. If not set, the owner will not be set. "
            + "You may also use expression language such as ${file.owner}. If the value is invalid, the processor will not be invalid but "
            + "will fail to change the owner of the file.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    public static final PropertyDescriptor REMOTE_GROUP = new PropertyDescriptor.Builder()
        .name("Remote Group")
        .description("Integer value representing the Group ID to set on the file after transferring it. If not set, the group will not be set. "
            + "You may also use expression language such as ${file.group}. If the value is invalid, the processor will not be invalid but "
            + "will fail to change the group of the file.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Batch Size")
        .description("The maximum number of FlowFiles to send in a single connection")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("500")
        .build();

}
