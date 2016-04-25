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
package org.apache.nifi.processors.alluxio;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ExistsOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UnmountOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.wire.FileInfo;

public class FileSystemMock implements FileSystem {

    private String directory = AlluxioTest.TEST_DIR;

    @Override
    public void createDirectory(AlluxioURI arg0) throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public void createDirectory(AlluxioURI arg0, CreateDirectoryOptions arg1) throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public FileOutStream createFile(AlluxioURI arg0) throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
        File newFile = new File(this.directory + File.separator + arg0.getName());
        newFile.createNewFile();
        return new MockFileOutStream(arg0, null);
    }

    @Override
    public FileOutStream createFile(AlluxioURI arg0, CreateFileOptions arg1) throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
        return createFile(arg0);
    }

    @Override
    public void delete(AlluxioURI arg0) throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public void delete(AlluxioURI arg0, DeleteOptions arg1) throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public boolean exists(AlluxioURI arg0) throws InvalidPathException, IOException, AlluxioException {
        // nothing to do
        return false;
    }

    @Override
    public boolean exists(AlluxioURI arg0, ExistsOptions arg1) throws InvalidPathException, IOException, AlluxioException {
        // nothing to do
        return false;
    }

    @Override
    public void free(AlluxioURI arg0) throws FileDoesNotExistException, IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public void free(AlluxioURI arg0, FreeOptions arg1) throws FileDoesNotExistException, IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public URIStatus getStatus(AlluxioURI arg0) throws FileDoesNotExistException, IOException, AlluxioException {
        File file = new File(this.directory + arg0.getPath());
        if(!file.exists()) {
            throw new FileDoesNotExistException("file does not exist");
        }
        return createURIStatis(file);
    }

    @Override
    public URIStatus getStatus(AlluxioURI arg0, GetStatusOptions arg1) throws FileDoesNotExistException, IOException, AlluxioException {
        return getStatus(arg0);
    }

    @Override
    public List<URIStatus> listStatus(AlluxioURI arg0) throws FileDoesNotExistException, IOException, AlluxioException {
        List<URIStatus> list = new ArrayList<URIStatus>();
        File[] files = new File(this.directory).listFiles();
        for (File file : files) {
            list.add(createURIStatis(file));
        }
        return list;
    }

    private URIStatus createURIStatis(File file) {
        FileInfo info = new FileInfo();
        info.setPath(file.getPath());
        info.setName(file.getName());
        return new URIStatus(info);
    }

    @Override
    public List<URIStatus> listStatus(AlluxioURI arg0, ListStatusOptions arg1) throws FileDoesNotExistException, IOException, AlluxioException {
        // nothing to do
        return null;
    }

    @Override
    public void loadMetadata(AlluxioURI arg0) throws FileDoesNotExistException, IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public void loadMetadata(AlluxioURI arg0, LoadMetadataOptions arg1) throws FileDoesNotExistException, IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public void mount(AlluxioURI arg0, AlluxioURI arg1) throws IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public void mount(AlluxioURI arg0, AlluxioURI arg1, MountOptions arg2) throws IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public FileInStream openFile(AlluxioURI arg0) throws FileDoesNotExistException, IOException, AlluxioException {
        return new MockFileInStream(createURIStatis(new File(this.directory + arg0.getPath())), null);
    }

    @Override
    public FileInStream openFile(AlluxioURI arg0, OpenFileOptions arg1) throws FileDoesNotExistException, IOException, AlluxioException {
        return openFile(arg0);
    }

    @Override
    public void rename(AlluxioURI arg0, AlluxioURI arg1) throws FileDoesNotExistException, IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public void rename(AlluxioURI arg0, AlluxioURI arg1, RenameOptions arg2) throws FileDoesNotExistException, IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public void setAttribute(AlluxioURI arg0) throws FileDoesNotExistException, IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public void setAttribute(AlluxioURI arg0, SetAttributeOptions arg1) throws FileDoesNotExistException, IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public void unmount(AlluxioURI arg0) throws IOException, AlluxioException {
        // nothing to do
    }

    @Override
    public void unmount(AlluxioURI arg0, UnmountOptions arg1) throws IOException, AlluxioException {
        // nothing to do
    }

    private class MockFileOutStream extends FileOutStream {

        private FileOutputStream outStream;

        public MockFileOutStream(AlluxioURI arg0, OutStreamOptions arg1) throws IOException {
            super(FileSystemContext.create(), arg0, OutStreamOptions.defaults());
            this.outStream = new FileOutputStream(new File(AlluxioTest.TEST_DIR + File.separator + arg0.getName()));
        }

        @Override
        public void write(byte[] arg0, int arg1, int arg2) throws IOException {
            this.outStream.write(arg0, arg1, arg2);
        }

        @Override
        public void close() throws IOException {
            this.outStream.close();
        }

        @Override
        public void flush() throws IOException {
            this.outStream.flush();
        }

        @Override
        public void write(byte[] b) throws IOException {
            this.outStream.write(b);
        }

        @Override
        public void write(int arg0) throws IOException {
            this.outStream.write(arg0);
        }
    }

    private class MockFileInStream extends FileInStream {

        private FileInputStream inStream;

        public MockFileInStream(URIStatus status, InStreamOptions options) {
            super(status, InStreamOptions.defaults(), null);
            try {
                this.inStream = new FileInputStream(new File(AlluxioTest.TEST_DIR + File.separator + status.getName()));
            } catch (FileNotFoundException e) {
                // nothing to do
            }
        }

        @Override
        public void close() throws IOException {
            this.inStream.close();
        }

        @Override
        public int read() throws IOException {
            return this.inStream.read();
        }

        @Override
        public int read(byte[] arg0, int arg1, int arg2) throws IOException {
            return this.inStream.read(arg0, arg1, arg2);
        }

        @Override
        public int read(byte[] b) throws IOException {
            return this.inStream.read(b);
        }

        @Override
        public int available() throws IOException {
            return this.inStream.available();
        }

        @Override
        public synchronized void mark(int readlimit) {
            this.inStream.mark(readlimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            this.inStream.reset();
        }
    }
}
