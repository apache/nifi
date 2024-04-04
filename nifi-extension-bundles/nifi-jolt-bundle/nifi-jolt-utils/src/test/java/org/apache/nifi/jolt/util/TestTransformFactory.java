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

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import com.bazaarvoice.jolt.CardinalityTransform;
import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.Defaultr;
import com.bazaarvoice.jolt.JoltTransform;
import com.bazaarvoice.jolt.JsonUtils;
import com.bazaarvoice.jolt.Modifier;
import com.bazaarvoice.jolt.Removr;
import com.bazaarvoice.jolt.Shiftr;
import com.bazaarvoice.jolt.Sortr;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestTransformFactory {

    @Test
    void testGetChainTransform() throws Exception{
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/chainrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-chain",JsonUtils.jsonToObject(chainrSpec));
        assertInstanceOf(Chainr.class, transform);
    }

    @Test
    void testGetDefaultTransform() throws Exception{
        final String defaultrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/defaultrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-default",JsonUtils.jsonToObject(defaultrSpec));
        assertInstanceOf(Defaultr.class, transform);
    }

    @Test
    void testGetSortTransform() throws Exception{
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-sort",null);
        assertInstanceOf(Sortr.class, transform);
    }

    @Test
    void testGetShiftTransform() throws Exception{
        final String shiftrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/shiftrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-shift",JsonUtils.jsonToObject(shiftrSpec));
        assertInstanceOf(Shiftr.class, transform);
    }

    @Test
    void testGetRemoveTransform() throws Exception{
        final String removrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/removrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-remove",JsonUtils.jsonToObject(removrSpec));
        assertInstanceOf(Removr.class, transform);
    }

    @Test
    void testGetCardinalityTransform() throws Exception{
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/cardrSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-card",JsonUtils.jsonToObject(cardrSpec));
        assertInstanceOf(CardinalityTransform.class, transform);
    }

    @Test
    void testGetModifierDefaultTransform() throws Exception{
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/modifierDefaultSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-modify-default",JsonUtils.jsonToObject(cardrSpec));
        assertInstanceOf(Modifier.Defaultr.class, transform);
    }

    @Test
    void testGetModifierDefineTransform() throws Exception{
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/modifierDefineSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-modify-define",JsonUtils.jsonToObject(cardrSpec));
        assertInstanceOf(Modifier.Definr.class, transform);
    }

    @Test
    void testGetModifierOverwriteTransform() throws Exception{
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/modifierOverwriteSpec.json")));
        JoltTransform transform = TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-modify-overwrite",JsonUtils.jsonToObject(cardrSpec));
        assertInstanceOf(Modifier.Overwritr.class, transform);
    }

    @Test
    void testGetInvalidTransformWithNoSpec() {
        Exception e = assertThrows(Exception.class, () -> TransformFactory.getTransform(getClass().getClassLoader(), "jolt-transform-chain",null));
        assertEquals("JOLT Chainr expects a JSON array of objects - Malformed spec.", e.getLocalizedMessage());
    }

    @Test
    void testGetCustomTransformation() throws Exception{
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/chainrSpec.json")));
        Path jarFilePath = Paths.get("src/test/resources/TestTransformFactory/TestCustomJoltTransform.jar");
        URL[] urlPaths = new URL[1];
        urlPaths[0] = jarFilePath.toUri().toURL();
        ClassLoader customClassLoader = new URLClassLoader(urlPaths,this.getClass().getClassLoader());
        JoltTransform transform = TransformFactory.getCustomTransform(customClassLoader,"TestCustomJoltTransform",JsonUtils.jsonToObject(chainrSpec));
        assertNotNull(transform);
        assertEquals("TestCustomJoltTransform", transform.getClass().getName());
    }

    @Test
    void testGetCustomTransformationNotFound() throws Exception{
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/chainrSpec.json")));
        ClassNotFoundException cnf = assertThrows(ClassNotFoundException.class, () -> TransformFactory.getCustomTransform(this.getClass().getClassLoader(), "TestCustomJoltTransform", chainrSpec));
        assertEquals("TestCustomJoltTransform", cnf.getLocalizedMessage());
    }
}
