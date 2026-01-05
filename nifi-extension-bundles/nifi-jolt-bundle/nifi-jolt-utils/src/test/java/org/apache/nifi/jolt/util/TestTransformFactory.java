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

package org.apache.nifi.jolt.util;

import io.joltcommunity.jolt.CardinalityTransform;
import io.joltcommunity.jolt.Chainr;
import io.joltcommunity.jolt.Defaultr;
import io.joltcommunity.jolt.JoltTransform;
import io.joltcommunity.jolt.JsonUtils;
import io.joltcommunity.jolt.Modifier;
import io.joltcommunity.jolt.Shiftr;
import io.joltcommunity.jolt.Sortr;
import io.joltcommunity.jolt.removr.Removr;
import org.apache.nifi.processors.jolt.CustomTransformJarProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestTransformFactory {

    private static final String CUSTOM_CLASS_NAME = CustomTransformJarProvider.getCustomTransformClassName();
    private static final String MISSING_CUSTOM_CLASS_NAME = "org.apache.nifi.processors.jolt.MissingCustomJoltTransform";

    @TempDir
    Path tempDir;

    @Test
    void testGetChainTransform() throws Exception {
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/chainrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-chain", JsonUtils.jsonToObject(chainrSpec));
        assertInstanceOf(Chainr.class, transform);
    }

    @Test
    void testGetDefaultTransform() throws Exception {
        final String defaultrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/defaultrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-default", JsonUtils.jsonToObject(defaultrSpec));
        assertInstanceOf(Defaultr.class, transform);
    }

    @Test
    void testGetSortTransform() throws Exception {
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-sort", null);
        assertInstanceOf(Sortr.class, transform);
    }

    @Test
    void testGetShiftTransform() throws Exception {
        final String shiftrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/shiftrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-shift", JsonUtils.jsonToObject(shiftrSpec));
        assertInstanceOf(Shiftr.class, transform);
    }

    @Test
    void testGetRemoveTransform() throws Exception {
        final String removrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/removrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-remove", JsonUtils.jsonToObject(removrSpec));
        assertInstanceOf(Removr.class, transform);
    }

    @Test
    void testGetCardinalityTransform() throws Exception {
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/cardrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-card", JsonUtils.jsonToObject(cardrSpec));
        assertInstanceOf(CardinalityTransform.class, transform);
    }

    @Test
    void testGetModifierDefaultTransform() throws Exception {
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/modifierDefaultSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-modify-default", JsonUtils.jsonToObject(cardrSpec));
        assertInstanceOf(Modifier.Defaultr.class, transform);
    }

    @Test
    void testGetModifierDefineTransform() throws Exception {
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/modifierDefineSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-modify-define", JsonUtils.jsonToObject(cardrSpec));
        assertInstanceOf(Modifier.Definr.class, transform);
    }

    @Test
    void testGetModifierOverwriteTransform() throws Exception {
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/modifierOverwriteSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-modify-overwrite", JsonUtils.jsonToObject(cardrSpec));
        assertInstanceOf(Modifier.Overwritr.class, transform);
    }

    @Test
    void testGetInvalidTransformWithNoSpec() {
        Exception e = assertThrows(Exception.class, () -> TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-chain", null));
        assertEquals("JOLT Chainr expects a JSON array of objects - Malformed spec.", e.getLocalizedMessage());
    }

    @Test
    void testGetCustomTransformation() throws Exception {
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/chainrSpec.json")));
        Path jarFilePath = CustomTransformJarProvider.createCustomTransformJar(tempDir);
        URL[] urlPaths = new URL[1];
        urlPaths[0] = jarFilePath.toUri().toURL();
        try (URLClassLoader customClassLoader = new URLClassLoader(urlPaths, this.getClass().getClassLoader())) {
            JoltTransform transform = TransformFactory.getCustomTransform(customClassLoader, CUSTOM_CLASS_NAME, JsonUtils.jsonToObject(chainrSpec));
            assertNotNull(transform);
            assertEquals(CUSTOM_CLASS_NAME, transform.getClass().getName());
        }
    }

    @Test
    void testGetCustomTransformationNotFound() throws Exception {
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/chainrSpec.json")));
        ClassNotFoundException cnf = assertThrows(ClassNotFoundException.class,
                () -> TransformFactory.getCustomTransform(this.getClass().getClassLoader(), MISSING_CUSTOM_CLASS_NAME, chainrSpec));
        assertEquals(MISSING_CUSTOM_CLASS_NAME, cnf.getLocalizedMessage());
    }
}
