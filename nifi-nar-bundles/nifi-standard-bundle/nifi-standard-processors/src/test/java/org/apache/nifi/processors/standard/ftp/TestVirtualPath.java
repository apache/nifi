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
package org.apache.nifi.processors.standard.ftp;

import org.apache.nifi.processors.standard.ftp.filesystem.VirtualPath;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class TestVirtualPath {

    @Test
    public void testCreatePathStartingWithSlash() {
        // GIVEN
        String expectedPath = "/Directory1/Directory2".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/Directory2");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testCreatePathStartingWithDoubleSlash() {
        // GIVEN
        String expectedPath = "/Directory1".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("//Directory1");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testCreatePathEndingWithSlash() {
        // GIVEN
        String expectedPath = "/Directory1".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testCreatePathEndingWithDoubleSlash() {
        // GIVEN
        String expectedPath = "/Directory1".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("/Directory1//");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testCreatePathNotStartingWithSlash() {
        // GIVEN
        String expectedPath = "/Directory1/Directory2".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("Directory1/Directory2");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testCreatPathToRoot() {
        // GIVEN
        String expectedPath = File.separator;
        VirtualPath objectUnderTest = new VirtualPath("/");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testCreatePathToRootWithDoubleSlash() {
        // GIVEN
        String expectedPath = File.separator;
        VirtualPath objectUnderTest = new VirtualPath("//");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testCreatePathThatNeedsToBeResolved() {
        // GIVEN
        String expectedPath = "/Directory1/SubDirectory1".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("//Directory1/SubDirectory1/../SubDirectory1");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testCreatePathWithWhitespace() {
        // GIVEN
        String expectedPath = "/Directory 1".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("/Directory 1");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testCreatePathWithBackslashes() {
        // GIVEN
        String expectedPath = "/Directory1/SubDirectory1".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("\\Directory1\\SubDirectory1");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testCreatePathWithSpecialCharacters() {
        // GIVEN
        String expectedPath = "/űáú▣☃/SubDirectory1".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("/űáú▣☃/SubDirectory1");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testEmptyPathPointsToRoot() {
        // GIVEN
        String expectedPath = File.separator;
        VirtualPath objectUnderTest = new VirtualPath("");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testPathIsNormalized() {
        // GIVEN
        String expectedPath = "/Directory1/Directory2".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("/Directory1///Directory2\\\\Directory3/Directory4/../..");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testGetFileNameForRoot() {
        // GIVEN
        String expectedPath = File.separator;
        VirtualPath objectUnderTest = new VirtualPath("/");

        // WHEN, THEN
        assertEquals(expectedPath, objectUnderTest.getFileName());
    }

    @Test
    public void testGetFileNameForNonRoot() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/Directory2/file.txt");

        // WHEN
        String result = objectUnderTest.getFileName();

        // THEN
        assertEquals("file.txt", result);
    }

    @Test
    public void testGetParentForRoot() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("/");

        // WHEN, THEN
        assertNull(objectUnderTest.getParent());
    }

    @Test
    public void testGetParentForNonRoot() {
        // GIVEN
        String expectedPath = "/Directory1/Directory2".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/Directory2/file.txt");

        // WHEN
        VirtualPath parent = objectUnderTest.getParent();

        // THEN
        assertEquals(expectedPath, parent.toString());
    }

    @Test
    public void testResolveToARelativePath() {
        // GIVEN
        String expectedPath = "/Directory1/Directory2/Directory3/Directory4".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/Directory2");

        // WHEN
        String result = objectUnderTest.resolve("Directory3/Directory4").toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testResolveToParent() {
        // GIVEN
        String expectedPath = "/Directory1".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/Directory2");

        // WHEN
        String result = objectUnderTest.resolve("..").toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testResolveToAnAbsolutePath() {
        // GIVEN
        String expectedPath = "/Directory3/Directory4".replace('/', File.separatorChar);
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/Directory2");

        // WHEN
        String result = objectUnderTest.resolve("/Directory3/Directory4").toString();

        // THEN
        assertEquals(expectedPath, result);
    }

    @Test
    public void testEquals() {
        // GIVEN
        VirtualPath path1 = new VirtualPath("/Directory1/Directory2");
        VirtualPath path2 = new VirtualPath("/Directory1/Directory2");

        // WHEN, THEN
        assertEquals(path1, path2);
    }

    @Test
    public void testDoesNotEqual() {
        // GIVEN
        VirtualPath path1 = new VirtualPath("/Directory1/Directory2");
        VirtualPath path2 = new VirtualPath("/Directory1/Directory3");

        // WHEN, THEN
        assertNotEquals(path1, path2);
    }

}
