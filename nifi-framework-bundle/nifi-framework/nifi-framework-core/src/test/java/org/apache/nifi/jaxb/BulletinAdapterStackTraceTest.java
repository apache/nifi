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
package org.apache.nifi.jaxb;

import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.ComponentType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class BulletinAdapterStackTraceTest {

    @Test
    void testMarshalUnmarshalCarriesStackTrace() throws Exception {
        final Throwable t = new NullPointerException("npe");
        final Bulletin original = BulletinFactory.createBulletin(
                "g", "G", "id", ComponentType.PROCESSOR, "Name",
                "Category", "ERROR", "msg", "/G", null, t);

        final BulletinAdapter adapter = new BulletinAdapter();
        final AdaptedBulletin adapted = adapter.marshal(original);
        assertNotNull(adapted);
        assertEquals(original.getStackTrace(), adapted.getStackTrace(), "AdaptedBulletin must copy stackTrace");

        final Bulletin roundTrip = adapter.unmarshal(adapted);
        assertNotNull(roundTrip);
        assertEquals(original.getStackTrace(), roundTrip.getStackTrace(), "Unmarshalled Bulletin must preserve stackTrace");
    }
}

