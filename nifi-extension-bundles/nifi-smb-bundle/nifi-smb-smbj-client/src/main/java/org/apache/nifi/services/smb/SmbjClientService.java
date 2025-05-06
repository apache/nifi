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
import com.hierynomus.mserref.NtStatus;
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
import org.apache.nifi.logging.ComponentLog;
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
    private static final long UNCATEGORIZED_ERROR = -1L;

    private final Session session;
    private final DiskShare share;
    private final URI serviceLocation;
    private final ComponentLog logger;

    SmbjClientService(final Session session, final DiskShare share, final URI serviceLocation, final ComponentLog logger) {
        this.session = session;
        this.share = share;
        this.serviceLocation = serviceLocation;
        this.logger = logger;
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
    public Stream<SmbListableEntity> listFiles(final String directoryPath) {
        return Stream.of(directoryPath).flatMap(path -> {
            final Directory directory;
            try {
                directory = openDirectory(path);
            } catch (SMBApiException e) {
                if (e.getStatus() == NtStatus.STATUS_ACCESS_DENIED) {
                    logger.warn("Could not list directory [{}/{}] because the user does not have access to it.", serviceLocation, path, e);
                    return Stream.empty();
                } else if (e.getStatus() == NtStatus.STATUS_BAD_NETWORK_NAME) {
                    // DFS resolution may return STATUS_BAD_NETWORK_NAME if the user does not have access at share level
                    logger.warn("Could not list directory [{}/{}] because the share does not exist or the user does not have access to it.", serviceLocation, path, e);
                    return Stream.empty();
                } else {
                    throw e;
                }
            }

            return stream(directory::spliterator, 0, false)
                    .map(entity -> buildSmbListableEntity(entity, path, serviceLocation))
                    .filter(entity -> !specialDirectory(entity))
                    .flatMap(listable -> listable.isDirectory() ? listFiles(listable.getPathWithName())
                            : Stream.of(listable))
                    .onClose(directory::close);
        });
    }

    @Override
    public void ensureDirectory(final String directoryPath) {
        try {
            final int lastDirectorySeparatorPosition = directoryPath.lastIndexOf("/");
            if (lastDirectorySeparatorPosition > 0) {
                ensureDirectory(directoryPath.substring(0, lastDirectorySeparatorPosition));
            }

            if (!share.folderExists(directoryPath)) {
                try {
                    share.mkdir(directoryPath);
                } catch (SMBApiException e) {
                    if (e.getStatus() == NtStatus.STATUS_OBJECT_NAME_COLLISION) {
                        if (!share.folderExists(directoryPath)) {
                            throw e;
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw wrapException(e);
        }
    }

    @Override
    public void readFile(final String filePath, final OutputStream outputStream) throws IOException {
        try (File file = share.openFile(
                filePath,
                EnumSet.of(AccessMask.GENERIC_READ),
                EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL),
                EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ),
                SMB2CreateDisposition.FILE_OPEN,
                EnumSet.of(SMB2CreateOptions.FILE_SEQUENTIAL_ONLY))
        ) {
            file.read(outputStream);
        } catch (Exception e) {
            throw wrapException(e);
        } finally {
            outputStream.close();
        }
    }

    @Override
    public void moveFile(final String filePath, final String directoryPath) {
        try (File file = share.openFile(
                filePath,
                EnumSet.of(AccessMask.GENERIC_WRITE, AccessMask.DELETE),
                EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL),
                EnumSet.noneOf(SMB2ShareAccess.class),
                SMB2CreateDisposition.FILE_OPEN,
                EnumSet.of(SMB2CreateOptions.FILE_SEQUENTIAL_ONLY))
        ) {
            final String[] parts = filePath.split("/");
            // rename operation on Windows requires \ (backslash) path separator
            final String newFilePath = directoryPath.replace('/', '\\') + "\\" + parts[parts.length - 1];
            file.rename(newFilePath);
        } catch (Exception e) {
            throw wrapException(e);
        }
    }

    @Override
    public void deleteFile(final String filePath) {
        try {
            share.rm(filePath);
        } catch (Exception e) {
            throw wrapException(e);
        }
    }

    private SmbListableEntity buildSmbListableEntity(final FileIdBothDirectoryInformation info, final String path, final URI serviceLocation) {
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

    private Directory openDirectory(final String path) {
        return share.openDirectory(
                path,
                EnumSet.of(AccessMask.GENERIC_READ),
                EnumSet.of(FileAttributes.FILE_ATTRIBUTE_DIRECTORY),
                EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ),
                SMB2CreateDisposition.FILE_OPEN,
                EnumSet.of(SMB2CreateOptions.FILE_DIRECTORY_FILE)
        );
    }

    private boolean specialDirectory(final SmbListableEntity entity) {
        return SPECIAL_DIRECTORIES.contains(entity.getName());
    }

    private SmbException wrapException(final Exception e) {
        if (e instanceof SmbException) {
            return (SmbException) e;
        } else {
            final long errorCode = e instanceof SMBApiException ? ((SMBApiException) e).getStatusCode() : UNCATEGORIZED_ERROR;
            return new SmbException(e.getMessage(), errorCode, e);
        }
    }

}


