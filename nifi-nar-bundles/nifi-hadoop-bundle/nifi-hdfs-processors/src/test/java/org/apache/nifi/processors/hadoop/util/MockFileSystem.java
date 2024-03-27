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
package org.apache.nifi.processors.hadoop.util;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.Progressable;
import org.ietf.jgss.GSSException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MockFileSystem extends FileSystem {
    private final Map<Path, FileStatus> pathToStatus = new HashMap<>();
    private final Map<Path, List<AclEntry>> pathToAcl = new HashMap<>();
    private final Map<Path, Set<FileStatus>> fileStatuses = new HashMap<>();

    private boolean failOnOpen;
    private boolean failOnClose;
    private boolean failOnCreate;
    private boolean failOnFileStatus;
    private boolean failOnExists;


    public void setFailOnClose(final boolean failOnClose) {
        this.failOnClose = failOnClose;
    }

    public void setFailOnCreate(final boolean failOnCreate) {
        this.failOnCreate = failOnCreate;
    }

    public void setFailOnFileStatus(final boolean failOnFileStatus) {
        this.failOnFileStatus = failOnFileStatus;
    }

    public void setFailOnExists(final boolean failOnExists) {
        this.failOnExists = failOnExists;
    }

    public void setFailOnOpen(final boolean failOnOpen) {
        this.failOnOpen = failOnOpen;
    }

    public void setAcl(final Path path, final List<AclEntry> aclSpec) {
        pathToAcl.put(path, aclSpec);
    }

    @Override
    public AclStatus getAclStatus(final Path path) {
        return new AclStatus.Builder().addEntries(pathToAcl.getOrDefault(path, new ArrayList<>())).build();
    }

    @Override
    public URI getUri() {
        return URI.create("file:///");
    }

    @Override
    public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {
        if (failOnOpen) {
            throw new IOException(new GSSException(13));
        }
        return null;
    }

    @Override
    public FSDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite, final int bufferSize, final short replication,
                                     final long blockSize, final Progressable progress) throws IOException {
        if (failOnCreate) {
            // Simulate an AuthenticationException wrapped in an IOException
            throw new IOException(new AuthenticationException("test auth error"));
        }
        pathToStatus.put(f, newFile(f, permission));
        if(failOnClose) {
            return new FSDataOutputStream(new ByteArrayOutputStream(), new FileSystem.Statistics("")) {
                @Override
                public void close() throws IOException {
                    super.close();
                    throw new IOException("Fail on close");
                }
            };
        } else {
            return new FSDataOutputStream(new ByteArrayOutputStream(), new Statistics(""));
        }
    }

    @Override
    public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) {
        return null;
    }

    @Override
    public boolean rename(final Path src, final Path dst) {
        if (pathToStatus.containsKey(src)) {
            pathToStatus.put(dst, pathToStatus.remove(src));
        } else {
            return false;
        }
        return true;
    }

    @Override
    public boolean delete(final Path f, final boolean recursive) {
        if (pathToStatus.containsKey(f)) {
            pathToStatus.remove(f);
        } else {
            return false;
        }
        return true;
    }

    @Override
    public void setWorkingDirectory(final Path new_dir) {

    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(new File(".").getAbsolutePath());
    }

    @Override
    public boolean mkdirs(final Path f, final FsPermission permission) {
        return false;
    }

    @Override
    public boolean mkdirs(Path f) {
        pathToStatus.put(f, newDir(f));
        return true;
    }

    @Override
    public FileStatus getFileStatus(final Path path) throws IOException {
        if (failOnFileStatus) {
            throw new IOException(new GSSException(13));
        }
        if (path != null && path.getName().startsWith("exception_")) {
            final String className = path.getName().substring("exception_".length());
            final IOException exception;
            try {
                exception = (IOException) Class.forName(className).getDeclaredConstructor().newInstance();
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
            throw exception;
        }

        final FileStatus fileStatus = pathToStatus.get(path);
        if (fileStatus == null) {
            throw new FileNotFoundException();
        }
        return fileStatus;
    }

    @Override
    public boolean exists(Path f) throws IOException {
        if (failOnExists) {
            throw new IOException(new GSSException(13));
        }
        return pathToStatus.containsKey(f);
    }

    public FileStatus newFile(Path p, FsPermission permission) {
        return new FileStatus(100L, false, 3, 128 * 1024 * 1024, 1523456000000L, 1523457000000L, permission, "owner", "group", p);
    }

    public FileStatus newDir(Path p) {
        return new FileStatus(1L, true, 3, 128 * 1024 * 1024, 1523456000000L, 1523457000000L, perms((short) 0755), "owner", "group", (Path)null, p, true, false, false);
    }

    public FileStatus newFile(String p) {
        return new FileStatus(100L, false, 3, 128*1024*1024, 1523456000000L, 1523457000000L, perms((short)0644), "owner", "group", new Path(p));
    }
    public FileStatus newDir(String p) {
        return new FileStatus(1L, true, 3, 128*1024*1024, 1523456000000L, 1523457000000L, perms((short)0755), "owner", "group", new Path(p));
    }

    @Override
    public long getDefaultBlockSize(Path f) {
        return 33554432L;
    }

    public void addFileStatus(final FileStatus parent, final FileStatus child) {
        Set<FileStatus> children = fileStatuses.computeIfAbsent(parent.getPath(), k -> new HashSet<>());
        if (child != null) {
            children.add(child);
            if (child.isDirectory() && !fileStatuses.containsKey(child.getPath())) {
                fileStatuses.put(child.getPath(), new HashSet<>());
            }
        }

        pathToStatus.put(parent.getPath(), parent);
        pathToStatus.put(child.getPath(), child);
    }

    @Override
    public FileStatus[] listStatus(final Path f) throws IOException {
        if (!fileStatuses.containsKey(f)) {
            throw new FileNotFoundException();
        }

        if (f.getName().startsWith("list_exception_")) {
            final String className = f.getName().substring("list_exception_".length());
            final IOException exception;
            try {
                exception = (IOException) Class.forName(className).getDeclaredConstructor().newInstance();
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
            throw exception;
        }

        final Set<FileStatus> statuses = fileStatuses.get(f);
        if (statuses == null) {
            return new FileStatus[0];
        }

        for (FileStatus s : statuses) {
            getFileStatus(s.getPath()); //support exception handling only.
        }

        return statuses.toArray(new FileStatus[0]);
    }

    @Override
    @SuppressWarnings("deprecation")
    public long getDefaultBlockSize() {
        return 1024L;
    }

    @Override
    @SuppressWarnings("deprecation")
    public short getDefaultReplication() {
        return 1;
    }


    private static FsPermission perms(short p) {
        return new FsPermission(p);
    }
}