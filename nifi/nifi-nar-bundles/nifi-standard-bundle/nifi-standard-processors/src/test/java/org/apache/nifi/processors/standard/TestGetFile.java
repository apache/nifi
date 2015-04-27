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

import org.apache.nifi.processors.standard.GetFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestGetFile {

    @Test
    public void testFilePickedUp() throws IOException {
        final File directory = new File("target/test/data/in");
        deleteDirectory(directory);
        assertTrue("Unable to create test data directory " + directory.
                getAbsolutePath(), directory.exists() || directory.mkdirs());

        final File inFile = new File("src/test/resources/hello.txt");
        final Path inPath = inFile.toPath();
        final File destFile = new File(directory, inFile.getName());
        final Path targetPath = destFile.toPath();
        final Path absTargetPath = targetPath.toAbsolutePath();
        final String absTargetPathStr = absTargetPath.getParent() + "/";
        Files.copy(inPath, targetPath);

        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        runner.setProperty(GetFile.DIRECTORY, directory.getAbsolutePath());
        runner.run();

        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.
                getFlowFilesForRelationship(GetFile.REL_SUCCESS);
        successFiles.get(0).
                assertContentEquals("Hello, World!".getBytes("UTF-8"));

        final String path = successFiles.get(0).
                getAttribute("path");
        assertEquals("/", path);
        final String absolutePath = successFiles.get(0).
                getAttribute(CoreAttributes.ABSOLUTE_PATH.key());
        assertEquals(absTargetPathStr, absolutePath);
    }

    private void deleteDirectory(final File directory) throws IOException {
        if (directory.exists()) {
            for (final File file : directory.listFiles()) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                }

                assertTrue("Could not delete " + file.getAbsolutePath(), file.
                        delete());
            }
        }
    }

    @Test
    public void testTodaysFilesPickedUp() throws IOException {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd", Locale.US);
        final String dirStruc = sdf.format(new Date());

        final File directory = new File("target/test/data/in/" + dirStruc);
        deleteDirectory(directory);
        assertTrue("Unable to create test data directory " + directory.
                getAbsolutePath(), directory.exists() || directory.mkdirs());

        final File inFile = new File("src/test/resources/hello.txt");
        final Path inPath = inFile.toPath();
        final File destFile = new File(directory, inFile.getName());
        final Path targetPath = destFile.toPath();
        Files.copy(inPath, targetPath);

        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        runner.
                setProperty(GetFile.DIRECTORY, "target/test/data/in/${now():format('yyyy/MM/dd')}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.
                getFlowFilesForRelationship(GetFile.REL_SUCCESS);
        successFiles.get(0).
                assertContentEquals("Hello, World!".getBytes("UTF-8"));
    }

    @Test
    public void testPath() throws IOException {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd/", Locale.US);
        final String dirStruc = sdf.format(new Date());

        final File directory = new File("target/test/data/in/" + dirStruc);
        deleteDirectory(new File("target/test/data/in"));
        assertTrue("Unable to create test data directory " + directory.
                getAbsolutePath(), directory.exists() || directory.mkdirs());

        final File inFile = new File("src/test/resources/hello.txt");
        final Path inPath = inFile.toPath();
        final File destFile = new File(directory, inFile.getName());
        final Path targetPath = destFile.toPath();
        final Path absTargetPath = targetPath.toAbsolutePath();
        final String absTargetPathStr = absTargetPath.getParent().
                toString() + "/";
        Files.copy(inPath, targetPath);

        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        runner.setProperty(GetFile.DIRECTORY, "target/test/data/in");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.
                getFlowFilesForRelationship(GetFile.REL_SUCCESS);
        successFiles.get(0).
                assertContentEquals("Hello, World!".getBytes("UTF-8"));

        final String path = successFiles.get(0).
                getAttribute("path");
        assertEquals(dirStruc, path.replace('\\', '/'));
        final String absolutePath = successFiles.get(0).
                getAttribute(CoreAttributes.ABSOLUTE_PATH.key());
        assertEquals(absTargetPathStr, absolutePath);
    }

    @Test
    public void testAttributes() throws IOException {
        final File directory = new File("target/test/data/in/");
        deleteDirectory(directory);
        assertTrue("Unable to create test data directory " + directory.
                getAbsolutePath(), directory.exists() || directory.mkdirs());

        final File inFile = new File("src/test/resources/hello.txt");
        final Path inPath = inFile.toPath();
        final File destFile = new File(directory, inFile.getName());
        final Path targetPath = destFile.toPath();
        Files.copy(inPath, targetPath);

        boolean verifyLastModified = false;
        try {
            destFile.setLastModified(1000000000);
            verifyLastModified = true;
        } catch (Exception donothing) {
        }

        boolean verifyPermissions = false;
        try {
            Files.setPosixFilePermissions(targetPath, PosixFilePermissions.
                    fromString("r--r-----"));
            verifyPermissions = true;
        } catch (Exception donothing) {
        }

        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        runner.setProperty(GetFile.DIRECTORY, "target/test/data/in");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.
                getFlowFilesForRelationship(GetFile.REL_SUCCESS);

        if (verifyLastModified) {
            try {
                final DateFormat formatter = new SimpleDateFormat(GetFile.FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                final Date fileModifyTime = formatter.parse(successFiles.get(0).
                        getAttribute("file.lastModifiedTime"));
                assertEquals(new Date(1000000000), fileModifyTime);
            } catch (ParseException e) {
                fail();
            }
        }
        if (verifyPermissions) {
            successFiles.get(0).
                    assertAttributeEquals("file.permissions", "r--r-----");
        }
    }
}
