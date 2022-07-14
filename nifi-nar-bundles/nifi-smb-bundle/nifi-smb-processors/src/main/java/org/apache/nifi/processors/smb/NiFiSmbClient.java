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
package org.apache.nifi.processors.smb;

import static java.util.Arrays.asList;
import static java.util.stream.StreamSupport.stream;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2CreateOptions;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.Directory;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

public class NiFiSmbClient {

    private static final List<String> SPECIAL_DIRECTORIES = asList(".", "..");

    private final Session session;
    private final DiskShare share;

    NiFiSmbClient(Session session, DiskShare share) {
        this.session = session;
        this.share = share;
    }

    static String unifyDirectorySeparators(String path) {
        return path.replace('/', '\\');
    }

    public void close() {
        try {
            session.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Stream<SmbListableEntity> listRemoteFiles(SmbListableEntity listable) {
        return listRemoteFiles(listable.getPathWithName());
    }

    Stream<SmbListableEntity> listRemoteFiles(String path) {
        final Directory directory = openDirectory(path);
        return stream(directory::spliterator, 0, false)
                .map(entity -> buildSmbListableEntity(entity, path))
                .filter(this::specialDirectory)
                .flatMap(listable -> listable.isDirectory() ? listRemoteFiles(listable) : Stream.of(listable))
                .onClose(directory::close);
    }

    private SmbListableEntity buildSmbListableEntity(FileIdBothDirectoryInformation info, String path) {
        return SmbListableEntity.builder()
                .setName(info.getFileName())
                .setShortName(info.getShortName())
                .setPath(path)
                .setTimestamp(info.getLastWriteTime().toEpochMillis())
                .setCreationTime(info.getCreationTime().toEpochMillis())
                .setChangeTime(info.getChangeTime().toEpochMillis())
                .setLastAccessTime(info.getLastAccessTime().toEpochMillis())
                .setDirectory((info.getFileAttributes() & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue()) != 0)
                .setSize(info.getEndOfFile())
                .setAllocationSize(info.getAllocationSize())
                .build();
    }

    void createDirectory(String path) {
        final int lastDirectorySeparatorPosition = path.lastIndexOf("\\");
        if (lastDirectorySeparatorPosition > 0) {
            createDirectory(path.substring(0, lastDirectorySeparatorPosition));
        }
        if (!share.folderExists(path)) {
            share.mkdir(path);
        }
    }

    OutputStream getOutputStreamForFile(String pathAndFileName) {
        try {
            final File file = share.openFile(
                    pathAndFileName,
                    EnumSet.of(AccessMask.GENERIC_WRITE),
                    EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL),
                    EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ, SMB2ShareAccess.FILE_SHARE_WRITE,
                            SMB2ShareAccess.FILE_SHARE_DELETE),
                    SMB2CreateDisposition.FILE_OVERWRITE_IF,
                    EnumSet.of(SMB2CreateOptions.FILE_WRITE_THROUGH));
            final OutputStream smbOutputStream = file.getOutputStream();
            return new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    smbOutputStream.write(b);
                }

                @Override
                public void flush() throws IOException {
                    smbOutputStream.flush();
                }

                @Override
                public void close() throws IOException {
                    smbOutputStream.close();
                    file.close();
                }
            };
        } catch (SMBApiException s) {
            throw new RuntimeException("Could not open " + pathAndFileName + " for writing", s);
        }
    }

    Directory openDirectory(String path) {
        try {
            return share.openDirectory(
                    path,
                    EnumSet.of(AccessMask.GENERIC_READ),
                    EnumSet.of(FileAttributes.FILE_ATTRIBUTE_DIRECTORY),
                    EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ),
                    SMB2CreateDisposition.FILE_OPEN,
                    EnumSet.of(SMB2CreateOptions.FILE_DIRECTORY_FILE)
            );
        } catch (SMBApiException s) {
            throw new RuntimeException("Could not open directory " + path + " due to " + s.getMessage(), s);
        }
    }

    private boolean specialDirectory(SmbListableEntity entity) {
        return !SPECIAL_DIRECTORIES.contains(entity.getName());
    }

}


