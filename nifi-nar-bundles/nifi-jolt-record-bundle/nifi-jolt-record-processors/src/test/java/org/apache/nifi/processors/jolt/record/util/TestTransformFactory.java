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

package org.apache.nifi.processors.jolt.record.util;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import com.bazaarvoice.jolt.CardinalityTransform;
import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.Defaultr;
import com.bazaarvoice.jolt.JoltTransform;
import com.bazaarvoice.jolt.JsonUtils;
import com.bazaarvoice.jolt.Modifier;
import com.bazaarvoice.jolt.Removr;
import com.bazaarvoice.jolt.Shiftr;
import com.bazaarvoice.jolt.Sortr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestTransformFactory {


    @Test
    public void testGetChainTransform() throws Exception {
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/chainrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-chain", JsonUtils.jsonToObject(chainrSpec));
        assertTrue(transform instanceof Chainr);
    }

    @Test
    public void testGetDefaultTransform() throws Exception {
        final String defaultrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/defaultrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-default", JsonUtils.jsonToObject(defaultrSpec));
        assertTrue(transform instanceof Defaultr);
    }

    @Test
    public void testGetSortTransform() throws Exception {
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-sort", null);
        assertTrue(transform instanceof Sortr);
    }

    @Test
    public void testGetShiftTransform() throws Exception {
        final String shiftrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/shiftrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-shift", JsonUtils.jsonToObject(shiftrSpec));
        assertTrue(transform instanceof Shiftr);
    }

    @Test
    public void testGetRemoveTransform() throws Exception {
        final String removrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/removrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-remove", JsonUtils.jsonToObject(removrSpec));
        assertTrue(transform instanceof Removr);
    }

    @Test
    public void testGetCardinalityTransform() throws Exception {
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/cardrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-card", JsonUtils.jsonToObject(cardrSpec));
        assertTrue(transform instanceof CardinalityTransform);
    }

    @Test
    public void testGetModifierDefaultTransform() throws Exception {
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/modifierDefaultSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-modify-default", JsonUtils.jsonToObject(cardrSpec));
        assertTrue(transform instanceof Modifier.Defaultr);
    }

    @Test
    public void testGetModifierDefineTransform() throws Exception {
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/modifierDefineSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-modify-define", JsonUtils.jsonToObject(cardrSpec));
        assertTrue(transform instanceof Modifier.Definr);
    }

    @Test
    public void testGetModifierOverwriteTransform() throws Exception {
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/modifierOverwriteSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-modify-overwrite", JsonUtils.jsonToObject(cardrSpec));
        assertTrue(transform instanceof Modifier.Overwritr);
    }

    @Test
    public void testGetInvalidTransformWithNoSpec() {
        try {
            TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-chain", null);
        } catch (Exception e) {
            assertEquals("JOLT Chainr expects a JSON array of objects - Malformed spec.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testGetCustomTransformation() throws Exception {
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/chainrSpec.json")));
        Path jarFilePath = Paths.get("src/test/resources/TestTransformFactory/TestCustomJoltTransform.jar");
        URL[] urlPaths = new URL[1];
        urlPaths[0] = jarFilePath.toUri().toURL();
        ClassLoader customClassLoader = new URLClassLoader(urlPaths, this.getClass().getClassLoader());
        JoltTransform transform = TransformFactory.getCustomTransform(customClassLoader, "TestCustomJoltTransform", JsonUtils.jsonToObject(chainrSpec));
        assertNotNull(transform);
        assertEquals("TestCustomJoltTransform", transform.getClass().getName());
    }

    @Test
    public void testGetCustomTransformationNotFound() throws Exception {
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/chainrSpec.json")));
        try {
            TransformFactory.getCustomTransform(this.getClass().getClassLoader(), "TestCustomJoltTransform", chainrSpec);
        } catch (ClassNotFoundException cnf) {
            assertEquals("TestCustomJoltTransform", cnf.getLocalizedMessage());
        }
    }


}
