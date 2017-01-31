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
package org.apache.nifi.bundle;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class BundleCoordinateTest {

    @Test
    public void testConstructorAndEquals() {
        final String group = "group1";
        final String id = "id1";
        final String version = "v1";

        final BundleCoordinate coordinate = new BundleCoordinate(group, id, version);
        assertEquals(group, coordinate.getGroup());
        assertEquals(id, coordinate.getId());
        assertEquals(version, coordinate.getVersion());
        assertEquals(group + ":" + id + ":" + version, coordinate.getCoordinate());

        final BundleCoordinate coordinate2 = new BundleCoordinate(group, id, version);
        assertEquals(coordinate, coordinate2);
    }

    @Test(expected = IllegalStateException.class)
    public void testIdRequired() {
        final String group = "group1";
        final String id = null;
        final String version = "v1";
        new BundleCoordinate(group, id, version);
    }

    @Test
    public void testDefaultGroup() {
        final String group = null;
        final String id = "id1";
        final String version = "v1";

        final BundleCoordinate coordinate = new BundleCoordinate(group, id, version);
        assertEquals(BundleCoordinate.DEFAULT_GROUP, coordinate.getGroup());
        assertEquals(id, coordinate.getId());
        assertEquals(version, coordinate.getVersion());
        assertEquals(BundleCoordinate.DEFAULT_GROUP + ":" + id + ":" + version, coordinate.getCoordinate());
    }

    @Test
    public void testVersionRequired() {
        final String group = "group1";
        final String id = "id1";
        final String version = null;

        final BundleCoordinate coordinate = new BundleCoordinate(group, id, version);
        assertEquals(group, coordinate.getGroup());
        assertEquals(id, coordinate.getId());
        assertEquals(BundleCoordinate.DEFAULT_VERSION, coordinate.getVersion());
        assertEquals(group + ":" + id + ":" + BundleCoordinate.DEFAULT_VERSION, coordinate.getCoordinate());
    }
}
