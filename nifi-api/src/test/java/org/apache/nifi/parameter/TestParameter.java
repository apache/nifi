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

package org.apache.nifi.parameter;

import org.apache.nifi.asset.Asset;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestParameter {

    @Test
    public void testCreateParameterWithValue() {
        final Parameter parameter = new Parameter.Builder()
            .name("A")
            .value("value")
            .build();

        assertEquals("A", parameter.getDescriptor().getName());
        assertEquals("value", parameter.getValue());
        assertNotNull(parameter.getReferencedAssets());
        assertTrue(parameter.getReferencedAssets().isEmpty());
    }

    @Test
    public void testCreateParameterWithSingleReference() {
        final File file = new File("file");
        final Asset asset = new MockAsset("id", "name", file);

        final Parameter parameter = new Parameter.Builder()
            .name("A")
            .referencedAssets(List.of(asset))
            .build();

        assertEquals("A", parameter.getDescriptor().getName());
        assertEquals(file.getAbsolutePath(), parameter.getValue());
        assertNotNull(parameter.getReferencedAssets());
        assertEquals(1, parameter.getReferencedAssets().size());
        assertEquals(asset, parameter.getReferencedAssets().getFirst());
    }

    @Test
    public void testCreateParameterWithMultipleReferences() {
        final File file1 = new File("file1");
        final File file2 = new File("file2");
        final File file3 = new File("file3");
        final Asset asset1 = new MockAsset("id1", "name1", file1);
        final Asset asset2 = new MockAsset("id2", "name2", file2);
        final Asset asset3 = new MockAsset("id3", "name3", file3);

        final Parameter parameter = new Parameter.Builder()
            .name("A")
            .referencedAssets(List.of(asset1, asset2, asset3))
            .build();

        assertEquals("A", parameter.getDescriptor().getName());
        assertEquals(file1.getAbsolutePath() + "," + file2.getAbsolutePath() + "," + file3.getAbsolutePath(), parameter.getValue());
        assertNotNull(parameter.getReferencedAssets());
        assertEquals(3, parameter.getReferencedAssets().size());
    }

    @Test
    public void testCreateParameterWithValueThenAsset() {
        final File file = new File("file");
        final Asset asset = new MockAsset("id", "name", file);

        final Parameter parameter = new Parameter.Builder()
            .name("A")
            .value("value")
            .referencedAssets(List.of(asset))
            .build();

        assertEquals("A", parameter.getDescriptor().getName());
        assertEquals(file.getAbsolutePath(), parameter.getValue());
        assertNotNull(parameter.getReferencedAssets());
        assertEquals(1, parameter.getReferencedAssets().size());
        assertEquals(asset, parameter.getReferencedAssets().getFirst());
    }

    @Test
    public void testCreateParameterAssetThenValue() {
        final File file = new File("file");
        final Asset asset = new MockAsset("id", "name", file);

        final Parameter parameter = new Parameter.Builder()
            .name("A")
            .referencedAssets(List.of(asset))
            .value("value")
            .build();

        assertEquals("A", parameter.getDescriptor().getName());
        assertEquals("value", parameter.getValue());
        assertNotNull(parameter.getReferencedAssets());
        assertEquals(0, parameter.getReferencedAssets().size());
    }

    @Test
    public void testCreateParameterFromOtherThenOverrideWithAsset() {
        final File file = new File("file");
        final Asset asset = new MockAsset("id", "name", file);

        final Parameter original = new Parameter.Builder()
            .name("A")
            .value("value")
            .build();

        final Parameter parameter = new Parameter.Builder()
            .fromParameter(original)
            .referencedAssets(List.of(asset))
            .build();

        assertEquals("A", parameter.getDescriptor().getName());
        assertNotNull(parameter.getReferencedAssets());
        assertEquals(1, parameter.getReferencedAssets().size());
        assertEquals(asset, parameter.getReferencedAssets().getFirst());
    }

    @Test
    public void testCreateParameterFromOtherThenOverrideWithValue() {
        final File file = new File("file");
        final Asset asset = new MockAsset("id", "name", file);

        final Parameter original = new Parameter.Builder()
            .name("A")
            .referencedAssets(List.of(asset))
            .build();

        final Parameter parameter = new Parameter.Builder()
            .fromParameter(original)
            .value("value")
            .build();

        assertEquals("A", parameter.getDescriptor().getName());
        assertEquals("value", parameter.getValue());
        assertNotNull(parameter.getReferencedAssets());
        assertEquals(0, parameter.getReferencedAssets().size());
    }

    @Test
    public void testCreateParameterFromOtherThenOverrideWithDifferentValue() {
        final Parameter original = new Parameter.Builder()
            .name("A")
            .value("value 1")
            .build();

        final Parameter parameter = new Parameter.Builder()
            .fromParameter(original)
            .value("value 2")
            .build();

        assertEquals("A", parameter.getDescriptor().getName());
        assertEquals("value 2", parameter.getValue());
        assertNotNull(parameter.getReferencedAssets());
        assertEquals(0, parameter.getReferencedAssets().size());
    }


    private static class MockAsset implements Asset {
        private final String identifier;
        private final String name;
        private final File file;

        public MockAsset(final String identifier, final String name, final File file) {
            this.identifier = identifier;
            this.name = name;
            this.file = file;
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public File getFile() {
            return file;
        }
    }
}
