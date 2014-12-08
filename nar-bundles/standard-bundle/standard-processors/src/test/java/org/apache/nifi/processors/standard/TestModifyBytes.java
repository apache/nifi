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

import org.apache.nifi.processors.standard.ModifyBytes;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TestModifyBytes {

    @Test
    public void testReturnEmptyFile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ModifyBytes());
        runner.setProperty(ModifyBytes.START_OFFSET, "1 MB");
        runner.setProperty(ModifyBytes.END_OFFSET, "1 MB");

        runner.enqueue(Paths.get("src/test/resources/TestModifyBytes/testFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyBytes.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ModifyBytes.REL_SUCCESS).get(0);
        out.assertContentEquals("".getBytes("UTF-8"));
    }

    @Test
    public void testReturnSameFile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ModifyBytes());
        runner.setProperty(ModifyBytes.START_OFFSET, "0 MB");
        runner.setProperty(ModifyBytes.END_OFFSET, "0 MB");

        runner.enqueue(Paths.get("src/test/resources/TestModifyBytes/testFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyBytes.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ModifyBytes.REL_SUCCESS).get(0);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestModifyBytes/testFile.txt")));
    }

    @Test
    public void testRemoveHeader() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ModifyBytes());
        runner.setProperty(ModifyBytes.START_OFFSET, "12 B"); //REMOVE - '<<<HEADER>>>'
        runner.setProperty(ModifyBytes.END_OFFSET, "0 MB");

        runner.enqueue(Paths.get("src/test/resources/TestModifyBytes/testFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyBytes.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ModifyBytes.REL_SUCCESS).get(0);
        final String outContent = new String(out.toByteArray(), StandardCharsets.UTF_8);
        System.out.println(outContent);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestModifyBytes/noHeader.txt")));
    }

    @Test
    public void testKeepFooter() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ModifyBytes());
        runner.setProperty(ModifyBytes.START_OFFSET, "181 B");
        runner.setProperty(ModifyBytes.END_OFFSET, "0 B");

        runner.enqueue(Paths.get("src/test/resources/TestModifyBytes/testFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyBytes.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ModifyBytes.REL_SUCCESS).get(0);
        final String outContent = new String(out.toByteArray(), StandardCharsets.UTF_8);
        System.out.println(outContent);
        out.assertContentEquals("<<<FOOTER>>>".getBytes("UTF-8"));
    }

    @Test
    public void testKeepHeader() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ModifyBytes());
        runner.setProperty(ModifyBytes.START_OFFSET, "0 B");
        runner.setProperty(ModifyBytes.END_OFFSET, "181 B");

        runner.enqueue(Paths.get("src/test/resources/TestModifyBytes/testFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyBytes.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ModifyBytes.REL_SUCCESS).get(0);
        out.assertContentEquals("<<<HEADER>>>".getBytes("UTF-8"));
    }

    @Test
    public void testRemoveFooter() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ModifyBytes());
        runner.setProperty(ModifyBytes.START_OFFSET, "0 B");
        runner.setProperty(ModifyBytes.END_OFFSET, "12 B");

        runner.enqueue(Paths.get("src/test/resources/TestModifyBytes/testFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyBytes.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ModifyBytes.REL_SUCCESS).get(0);
        final String outContent = new String(out.toByteArray(), StandardCharsets.UTF_8);
        System.out.println(outContent);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestModifyBytes/noFooter.txt")));
    }

    @Test
    public void testRemoveHeaderAndFooter() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ModifyBytes());
        runner.setProperty(ModifyBytes.START_OFFSET, "12 B");
        runner.setProperty(ModifyBytes.END_OFFSET, "12 B");

        runner.enqueue(Paths.get("src/test/resources/TestModifyBytes/testFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyBytes.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ModifyBytes.REL_SUCCESS).get(0);
        final String outContent = new String(out.toByteArray(), StandardCharsets.UTF_8);
        System.out.println(outContent);
        out.assertContentEquals(translateNewLines(new File("src/test/resources/TestModifyBytes/noFooter_noHeader.txt")));
    }

    @Test
    public void testReturnZeroByteFile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ModifyBytes());
        runner.setProperty(ModifyBytes.START_OFFSET, "97 B");
        runner.setProperty(ModifyBytes.END_OFFSET, "97 B");

        runner.enqueue(Paths.get("src/test/resources/TestModifyBytes/testFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyBytes.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ModifyBytes.REL_SUCCESS).get(0);
        out.assertContentEquals("".getBytes("UTF-8"));
    }

    @Test
    public void testDew() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ModifyBytes());
        runner.setProperty(ModifyBytes.START_OFFSET, "94 B");
        runner.setProperty(ModifyBytes.END_OFFSET, "96 B");

        runner.enqueue(Paths.get("src/test/resources/TestModifyBytes/testFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyBytes.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ModifyBytes.REL_SUCCESS).get(0);
        final String outContent = new String(out.toByteArray(), StandardCharsets.UTF_8);
        System.out.println(outContent);
        out.assertContentEquals("Dew".getBytes("UTF-8"));
    }

    private byte[] translateNewLines(final File file) throws IOException {
        return translateNewLines(file.toPath());
    }

    private byte[] translateNewLines(final Path path) throws IOException {
        final byte[] data = Files.readAllBytes(path);
        final String text = new String(data, StandardCharsets.UTF_8);
        return translateNewLines(text).getBytes(StandardCharsets.UTF_8);
    }

    private String translateNewLines(final String text) {
        final String lineSeparator = System.getProperty("line.separator");
        final Pattern pattern = Pattern.compile("\n", Pattern.MULTILINE);
        final String translated = pattern.matcher(text).replaceAll(lineSeparator);
        return translated;
    }
}
