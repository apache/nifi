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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ListFileUtil {

    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
    public static final String FILE_SIZE_ATTRIBUTE = "file.size";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    public static List<FileInfo> scanDirectory(final File path, final FileInfoFilter filter, final Boolean recurse,
                                         final Long minTimestamp) {
        final List<FileInfo> listing = new ArrayList<>();
        File[] files = path.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    if (recurse) {
                        listing.addAll(scanDirectory(file, filter, true, minTimestamp));
                    }
                } else {
                    FileInfo fileInfo = new FileInfo.Builder()
                            .directory(false)
                            .filename(file.getName())
                            .fullPathFileName(file.getAbsolutePath())
                            .size(file.length())
                            .lastModifiedTime(file.lastModified())
                            .build();
                    if ((minTimestamp == null || fileInfo.getLastModifiedTime() >= minTimestamp)
                            && filter.accept(file, fileInfo)) {
                        listing.add(fileInfo);
                    }
                }
            }
        }

        return listing;
    }

    public static Map<String, String> createAttributes(final String dirPath, final FileInfo fileInfo,
                                                   final boolean includeFileAttributes, final ComponentLog logger) {
        final Map<String, String> attributes = new HashMap<>();

        final String fullPath = fileInfo.getFullPathFileName();
        final File file = new File(fullPath);
        final Path filePath = file.toPath();
        final Path directoryPath = new File(dirPath).toPath();

        final Path relativePath = directoryPath.toAbsolutePath().relativize(filePath.getParent());
        String relativePathString = relativePath.toString();
        relativePathString = relativePathString.isEmpty() ? "." + File.separator : relativePathString + File.separator;

        final Path absPath = filePath.toAbsolutePath();
        final String absPathString = absPath.getParent().toString() + File.separator;

        final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);

        attributes.put(CoreAttributes.PATH.key(), relativePathString);
        attributes.put(CoreAttributes.FILENAME.key(), fileInfo.getFileName());
        attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), absPathString);
        attributes.put(FILE_SIZE_ATTRIBUTE, Long.toString(fileInfo.getSize()));
        attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, formatter.format(new Date(fileInfo.getLastModifiedTime())));

        if (includeFileAttributes) {
            try {
                FileStore store = Files.getFileStore(filePath);
                if (store.supportsFileAttributeView("basic")) {
                    try {
                        BasicFileAttributeView view = Files.getFileAttributeView(filePath, BasicFileAttributeView.class);
                        BasicFileAttributes attrs = view.readAttributes();
                        attributes.put(FILE_CREATION_TIME_ATTRIBUTE, formatter.format(new Date(attrs.creationTime().toMillis())));
                        attributes.put(FILE_LAST_ACCESS_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastAccessTime().toMillis())));
                    } catch (Exception ignore) {
                    } // allow other attributes if these fail
                }

                if (store.supportsFileAttributeView("owner")) {
                    try {
                        FileOwnerAttributeView view = Files.getFileAttributeView(filePath, FileOwnerAttributeView.class);
                        attributes.put(FILE_OWNER_ATTRIBUTE, view.getOwner().getName());
                    } catch (Exception ignore) {
                    } // allow other attributes if these fail
                }

                if (store.supportsFileAttributeView("posix")) {
                    try {
                        PosixFileAttributeView view = Files.getFileAttributeView(filePath, PosixFileAttributeView.class);
                        attributes.put(FILE_PERMISSIONS_ATTRIBUTE, PosixFilePermissions.toString(view.readAttributes().permissions()));
                        attributes.put(FILE_GROUP_ATTRIBUTE, view.readAttributes().group().getName());
                    } catch (Exception ignore) {
                    } // allow other attributes if these fail
                }
            } catch (IOException ioe) {
                // well then this FlowFile gets none of these attributes
                logger.warn("Error collecting attributes for file {}, message is {}", new Object[] {absPathString, ioe.getMessage()});
            }
        }

        return attributes;
    }

}
