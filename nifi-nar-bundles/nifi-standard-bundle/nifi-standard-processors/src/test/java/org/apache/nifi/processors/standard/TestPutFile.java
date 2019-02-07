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
package org.apache.nifi.processors.standard;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPutFile {

    private static final Logger logger = LoggerFactory.getLogger(TestPutFile.class);

    private TestRunner putFileRunner;

    private final String testFile = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "hello.txt";

    @Before
    public void setup() throws IOException{
        
        putFileRunner = TestRunners.newTestRunner(PutFile.class);
        putFileRunner.setProperty(PutFile.CHANGE_OWNER, System.getProperty("user.name"));
//        putFileRunner.setProperty(PutFile.CHANGE_GROUP, "group4");
        putFileRunner.setProperty(PutFile.CHANGE_PERMISSIONS, "rw-r-----");
        putFileRunner.setProperty(PutFile.CREATE_DIRS, "true");
        putFileRunner.setProperty(PutFile.DIRECTORY, "target/test/data/out/PutFile/1/2/3/4/5");
//        putFileRunner.setProperty(PutFile.CHANGE_LAST_MODIFIED_TIME, "false");

        putFileRunner.setValidateExpressionUsage(false);
    }
    
    @After
    public void tearDown() throws IOException {
//        emptyTestDirectory();
    }

    @Test
    public void testPutFile() throws IOException {
        emptyTestDirectory();

        Map<String,String> attributes = new HashMap<>();
        attributes.put("filename", "testfile.txt");

        putFileRunner.enqueue(Paths.get(testFile), attributes);
        putFileRunner.run();

        putFileRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);

        //verify directory exists
        Path newDirectory = Paths.get("target/test/data/out/PutFile/1/2/3/4/5");
        Path newFile = newDirectory.resolve("testfile.txt");
        Assert.assertTrue("New directory not created.", newDirectory.toAbsolutePath().toFile().exists());
        Assert.assertTrue("New File not created.", newFile.toAbsolutePath().toFile().exists());
        putFileRunner.clearTransferState();
    }

//    @Test
//    public void testPutFileConflictResolution() throws IOException {
//        emptyTestDirectory();
//
//        //Try transferring file with the same name as a directory, should fail in all cases
//        // except RESOLUTION of NONE
//        Path dir = Paths.get(sshTestServer.getVirtualFileSystemPath() + "nifi_test" );
//        Path dir2 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "nifi_test/testfile" );
//        Files.createDirectory(dir);
//        Files.createDirectory(dir2);
//
//        Map<String,String> attributes = new HashMap<>();
//        attributes.put("filename", "testfile");
//
//        putFileRunner.enqueue(Paths.get(testFile), attributes);
//        putFileRunner.run();
//
//        putFileRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 0);
//        putFileRunner.assertTransferCount(PutSFTP.REL_FAILURE, 1);
//
//        //Prepare by uploading test file
//        attributes = new HashMap<>();
//        attributes.put("filename", "testfile.txt");
//
//        putFileRunner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_REPLACE);
//
//        putFileRunner.enqueue(Paths.get(testFile), attributes);
//        putFileRunner.run();
//        putFileRunner.clearTransferState();
//
//        //set conflict resolution mode to REJECT
//        putFileRunner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_REJECT);
//
//        putFileRunner.enqueue(Paths.get(testFile), attributes);
//        putFileRunner.run();
//
//        putFileRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 0);
//        putFileRunner.assertTransferCount(PutSFTP.REL_REJECT, 1);
//        putFileRunner.clearTransferState();
//
//        //set conflict resolution mode to IGNORE
//        putFileRunner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_IGNORE);
//        putFileRunner.enqueue(Paths.get(testFile), attributes);
//        putFileRunner.run();
//
//        putFileRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);
//        putFileRunner.assertTransferCount(PutSFTP.REL_REJECT, 0);
//
//        putFileRunner.clearTransferState();
//
//        //set conflict resolution mode to FAIL
//        putFileRunner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_FAIL);
//        putFileRunner.enqueue(Paths.get(testFile), attributes);
//        putFileRunner.run();
//
//        putFileRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 0);
//        putFileRunner.assertTransferCount(PutSFTP.REL_REJECT, 0);
//        putFileRunner.assertTransferCount(PutSFTP.REL_FAILURE, 1);
//
//        putFileRunner.clearTransferState();
//    }
//
    
    private void emptyTestDirectory() throws IOException {
    	Files.walkFileTree(Paths.get("target/test/data/out/PutFile"), new FileVisitor<Path>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}