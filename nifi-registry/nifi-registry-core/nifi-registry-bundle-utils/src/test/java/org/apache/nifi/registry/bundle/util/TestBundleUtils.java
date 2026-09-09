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
package org.apache.nifi.registry.bundle.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class TestBundleUtils {

    @Test
    void testValidateCoordinateFieldAcceptsTypicalValues() {
        BundleUtils.validateCoordinateField("Group Id", "org.apache.nifi");
        BundleUtils.validateCoordinateField("Artifact Id", "nifi-standard-nar");
        BundleUtils.validateCoordinateField("Version", "2.0.0-SNAPSHOT");
        BundleUtils.validateCoordinateField("Version", "1.0.0+build.5");
    }

    @Test
    void testValidateCoordinateFieldRejectsInvalidValues() {
        assertThrows(IllegalArgumentException.class, () -> BundleUtils.validateCoordinateField("Group Id", null));
        assertThrows(IllegalArgumentException.class, () -> BundleUtils.validateCoordinateField("Group Id", "  "));
        assertThrows(IllegalArgumentException.class, () -> BundleUtils.validateCoordinateField("Group Id", "."));
        assertThrows(IllegalArgumentException.class, () -> BundleUtils.validateCoordinateField("Group Id", ".."));
        assertThrows(IllegalArgumentException.class, () -> BundleUtils.validateCoordinateField("Group Id", "org/apache"));
        assertThrows(IllegalArgumentException.class, () -> BundleUtils.validateCoordinateField("Artifact Id", "art\\ifact"));
        assertThrows(IllegalArgumentException.class, () -> BundleUtils.validateCoordinateField("Version", "1.0.0\0"));
    }
}
