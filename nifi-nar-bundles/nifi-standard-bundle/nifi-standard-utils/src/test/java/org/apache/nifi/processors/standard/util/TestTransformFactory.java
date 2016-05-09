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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;

import com.bazaarvoice.jolt.CardinalityTransform;
import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.Defaultr;
import com.bazaarvoice.jolt.JsonUtils;
import com.bazaarvoice.jolt.Removr;
import com.bazaarvoice.jolt.Shiftr;
import com.bazaarvoice.jolt.Sortr;
import com.bazaarvoice.jolt.Transform;

import static org.junit.Assert.assertTrue;

public class TestTransformFactory {


    @Test
    public void testGetChainTransform() throws IOException{
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/chainrSpec.json")));
        Transform transform = TransformFactory.getTransform("jolt-transform-chain",JsonUtils.jsonToObject(chainrSpec));
        assertTrue(transform instanceof Chainr);
    }

    @Test
    public void testGetDefaultTransform() throws IOException{
        final String defaultrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/defaultrSpec.json")));
        Transform transform = TransformFactory.getTransform("jolt-transform-default",JsonUtils.jsonToObject(defaultrSpec));
        assertTrue(transform instanceof Defaultr);
    }

    @Test
    public void testGetSortTransform() throws IOException{
        Transform transform = TransformFactory.getTransform("jolt-transform-sort",null);
        assertTrue(transform instanceof Sortr);
    }

    @Test
    public void testGetShiftTransform() throws IOException{
        final String shiftrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/shiftrSpec.json")));
        Transform transform = TransformFactory.getTransform("jolt-transform-shift",JsonUtils.jsonToObject(shiftrSpec));
        assertTrue(transform instanceof Shiftr);
    }

    @Test
    public void testGetRemoveTransform() throws IOException{
        final String removrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/removrSpec.json")));
        Transform transform = TransformFactory.getTransform("jolt-transform-remove",JsonUtils.jsonToObject(removrSpec));
        assertTrue(transform instanceof Removr);
    }

    @Test
    public void testGetCardinalityTransform() throws IOException{
        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformFactory/cardrSpec.json")));
        Transform transform = TransformFactory.getTransform("jolt-transform-card",JsonUtils.jsonToObject(cardrSpec));
        assertTrue(transform instanceof CardinalityTransform);
    }

    public void testGetInvalidTransformWithNoSpec() {
        try{
            TransformFactory.getTransform("jolt-transform-chain",null);
        }catch (Exception e){
            assertTrue(e.toString().equals("JOLT Chainr expects a JSON array of objects - Malformed spec."));
        }
    }

}
