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
package org.apache.nifi.services.smb;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

class SmbjClientService implements SmbClientService {

    private final static Logger LOGGER = LoggerFactory.getLogger(SmbjClientService.class);

    private static final List<String> SPECIAL_DIRECTORIES = asList(".", "..");
    private static final long UNCATEGORISED_ERROR = -1L;

    private final Session session;
    private final DiskShare share;
    private final URI serviceLocation;

    SmbjClientService(Session session, DiskShare share, URI serviceLocation) {
        this.session = session;
        this.share = share;
        this.serviceLocation = serviceLocation;
    }

    @Override
    public void close() {
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            LOGGER.error("Could not close session to {}", serviceLocation, e);
        }
    }

    @Override
    public Stream<SmbListableEntity> listRemoteFiles(String filePath) {
        return Stream.of(filePath).flatMap(path -> {
            final Directory directory = openDirectory(path);
            return stream(directory::spliterator, 0, false)
                    .map(entity -> buildSmbListableEntity(entity, path, serviceLocation))
                    .filter(entity -> !specialDirectory(entity))
                    .flatMap(listable -> listable.isDirectory() ? listRemoteFiles(listable.getPathWithName())
                            : Stream.of(listable))
                    .onClose(directory::close);
        });
    }

    @Override
    public void createDirectory(String path) {
        final int lastDirectorySeparatorPosition = path.lastIndexOf("/");
        if (lastDirectorySeparatorPosition > 0) {
            createDirectory(path.substring(0, lastDirectorySeparatorPosition));
        }
        if (!share.folderExists(path)) {
            share.mkdir(path);
        }
    }

    @Override
    public void readFile(String fileName, OutputStream outputStream) throws IOException {
        try (File f = share.openFile(
                fileName,
                EnumSet.of(AccessMask.GENERIC_READ),
                EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL),
                EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ),
                SMB2CreateDisposition.FILE_OPEN,
                EnumSet.of(SMB2CreateOptions.FILE_SEQUENTIAL_ONLY))
        ) {
            f.read(outputStream);
        } catch (SMBApiException a) {
            throw new SmbException(a.getMessage(), a.getStatusCode(), a);
        } catch (Exception e) {
            throw new SmbException(e.getMessage(), UNCATEGORISED_ERROR, e);
        } finally {
            outputStream.close();
        }
    }

    private SmbListableEntity buildSmbListableEntity(FileIdBothDirectoryInformation info, String path, URI serviceLocation) {
        return SmbListableEntity.builder()
                .setName(info.getFileName())
                .setShortName(info.getShortName())
                .setPath(path)
                .setLastModifiedTime(info.getLastWriteTime().toEpochMillis())
                .setCreationTime(info.getCreationTime().toEpochMillis())
                .setChangeTime(info.getChangeTime().toEpochMillis())
                .setLastAccessTime(info.getLastAccessTime().toEpochMillis())
                .setDirectory((info.getFileAttributes() & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue()) != 0)
                .setSize(info.getEndOfFile())
                .setAllocationSize(info.getAllocationSize())
                .setServiceLocation(serviceLocation)
                .build();
    }

    private Directory openDirectory(String path) {
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
        return SPECIAL_DIRECTORIES.contains(entity.getName());
    }

}


