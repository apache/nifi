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

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.StreamSupport.stream;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2CreateOptions;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.Directory;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.Share;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

public class SmbjClientService implements SmbClientService {

    private static final List<String> SPECIAL_DIRECTORIES = asList(".", "..");

    final private AuthenticationContext authenticationContext;
    final private SMBClient smbClient;

    private Connection connection;
    private Session session;
    private DiskShare share;

    public SmbjClientService(SMBClient smbClient, AuthenticationContext authenticationContext) {
        this.smbClient = smbClient;
        this.authenticationContext = authenticationContext;
    }

    public void connectToShare(String hostname, int port, String shareName) throws IOException {
        Share share;
        try {
            connection = smbClient.connect(hostname, port);
            session = connection.authenticate(authenticationContext);
            share = session.connectShare(shareName);
        } catch (Exception e) {
            close();
            throw new IOException("Could not connect to share " + format("%s:%d/%s", hostname, port, shareName), e);
        }
        if (share instanceof DiskShare) {
            this.share = (DiskShare) share;
        } else {
            close();
            throw new IllegalArgumentException("DiskShare not found. Share " +
                    share.getClass().getSimpleName() + " found on " + format("%s:%d/%s", hostname, port,
                    shareName));
        }
    }

    public void forceFullyCloseConnection() {
        try {
            if (connection != null) {
                connection.close(true);
            }
        } catch (IOException ignore) {
        } finally {
            connection = null;
        }
    }

    @Override
    public void close() {
        try {
            if (session != null) {
                session.close();
            }
        } catch (IOException ignore) {

        } finally {
            session = null;
        }
    }

    @Override
    public Stream<SmbListableEntity> listRemoteFiles(String filePath) {
        return Stream.of(filePath).flatMap(path -> {
            final Directory directory = openDirectory(path);
            return stream(directory::spliterator, 0, false)
                    .map(entity -> buildSmbListableEntity(entity, path))
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


