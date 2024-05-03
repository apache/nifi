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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestFileStatusIterator {
    @Mock
    private FileSystem mockHdfs;

    @Mock
    private UserGroupInformation mockUserGroupInformation;

    @Mock
    private FileStatus mockFileStatus1;

    @Mock
    private FileStatus mockFileStatus2;

    @Mock
    private FileStatus mockFileStatus3;

    private FileStatusIterable fileStatusIterable;

    @BeforeEach
    void setup() {
        fileStatusIterable = new FileStatusIterable(new Path("/path/to/files"), false, mockHdfs, mockUserGroupInformation);
    }

    @Test
    void pathWithNoFilesShouldReturnEmptyIterator() throws Exception {
        when(mockUserGroupInformation.doAs(any(PrivilegedExceptionAction.class))).thenReturn(new MockRemoteIterator());

        final Iterator<FileStatus> iterator = fileStatusIterable.iterator();

        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void pathWithMultipleFilesShouldReturnIteratorWithCorrectFiles() throws Exception {
        final FileStatus[] fileStatuses = {mockFileStatus1, mockFileStatus2, mockFileStatus3};
        setupFileStatusMocks(fileStatuses);


        final Iterator<FileStatus> iterator = fileStatusIterable.iterator();
        final Set<FileStatus> expectedFileStatuses = new HashSet<>(Arrays.asList(fileStatuses));
        final Set<FileStatus> actualFileStatuses = new HashSet<>();

        assertTrue(iterator.hasNext());
        actualFileStatuses.add(iterator.next());
        assertTrue(iterator.hasNext());
        actualFileStatuses.add(iterator.next());
        assertTrue(iterator.hasNext());
        actualFileStatuses.add(iterator.next());

        assertEquals(expectedFileStatuses, actualFileStatuses);

        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void getTotalFileCountWithMultipleFilesShouldReturnCorrectCount() throws Exception {
        final FileStatus[] fileStatuses = {mockFileStatus1, mockFileStatus2, mockFileStatus3};
        setupFileStatusMocks(fileStatuses);

        assertEquals(0, fileStatusIterable.getTotalFileCount());

        for (FileStatus ignored : fileStatusIterable) {
            // count files
        }

        assertEquals(3, fileStatusIterable.getTotalFileCount());
    }

    private void setupFileStatusMocks(FileStatus[] fileStatuses) throws IOException, InterruptedException {
        when(mockHdfs.listStatusIterator(any(Path.class))).thenReturn(new MockRemoteIterator(fileStatuses));

        when(mockUserGroupInformation.doAs(any(PrivilegedExceptionAction.class))).thenAnswer(invocation -> {
            // Get the provided lambda expression
            PrivilegedExceptionAction action = invocation.getArgument(0);

            // Invoke the lambda expression and return the result
            return action.run();
        });
    }

    private static class MockRemoteIterator implements RemoteIterator<FileStatus> {

        final Deque<FileStatus> deque;

        public MockRemoteIterator(FileStatus... fileStatuses) {
            deque = new ArrayDeque<>();
            Collections.addAll(deque, fileStatuses);
        }

        @Override
        public boolean hasNext() {
            return !deque.isEmpty();
        }

        @Override
        public FileStatus next() {
            return deque.pop();
        }
    }
}

