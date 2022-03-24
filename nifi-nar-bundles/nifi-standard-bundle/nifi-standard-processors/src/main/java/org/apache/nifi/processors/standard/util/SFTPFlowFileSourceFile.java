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

import net.schmizz.sshj.xfer.LocalFileFilter;
import net.schmizz.sshj.xfer.LocalSourceFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

/**
 * Implementation of SSHJ's LocalSourceFile so we can call 'put' using a FlowFile's InputStream.
 */
public class SFTPFlowFileSourceFile implements LocalSourceFile {

    private final String filename;
    private final InputStream inputStream;
    private final int permissions;

    public SFTPFlowFileSourceFile(final String filename, final InputStream inputStream) {
        this(filename, inputStream, 0);
    }

    public SFTPFlowFileSourceFile(final String filename, final InputStream inputStream, final int perms) {
        this.filename = filename;
        this.inputStream = inputStream;
        this.permissions = perms;
    }

    @Override
    public String getName() {
        return filename;
    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return inputStream;
    }

    @Override
    public int getPermissions() throws IOException {
        return permissions;
    }

    @Override
    public boolean isFile() {
        return true;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public Iterable<? extends LocalSourceFile> getChildren(LocalFileFilter filter) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public boolean providesAtimeMtime() {
        return false;
    }

    @Override
    public long getLastAccessTime() throws IOException {
        return 0;
    }

    @Override
    public long getLastModifiedTime() throws IOException {
        return 0;
    }
}
