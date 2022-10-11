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

package org.apache.nifi.c2.client.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.apache.nifi.c2.protocol.api.SupportedOperation;
import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.junit.jupiter.api.Test;

class ManifestHashProviderTest {
    private ManifestHashProvider manifestHashProvider = new ManifestHashProvider();

    @Test
    void testManifestHashChangesWhenManifestBundleChanges() {
        Bundle bundle1 = new Bundle("group1", "artifact1", "version1");
        Bundle bundle2 = new Bundle("group2", "artifact2", "version2");

        SupportedOperation supportedOperation1 = new SupportedOperation();
        supportedOperation1.setType(OperationType.HEARTBEAT);
        SupportedOperation supportedOperation2 = new SupportedOperation();
        supportedOperation2.setType(OperationType.ACKNOWLEDGE);

        String hash1 = manifestHashProvider.calculateManifestHash(Collections.singletonList(bundle1), Collections.singleton(supportedOperation1));
        assertNotNull(hash1);

        // same manifest should result in the same hash
        assertEquals(hash1, manifestHashProvider.calculateManifestHash(Collections.singletonList(bundle1), Collections.singleton(supportedOperation1)));

        // different manifest should result in hash change if only bundle change
        String hash2 = manifestHashProvider.calculateManifestHash(Collections.singletonList(bundle2), Collections.singleton(supportedOperation1));
        assertNotEquals(hash2, hash1);

        // different manifest should result in hash change if only supported operation change
        String hash3 = manifestHashProvider.calculateManifestHash(Collections.singletonList(bundle1), Collections.singleton(supportedOperation2));
        assertNotEquals(hash3, hash1);

        // different manifest with multiple bundles should result in hash change compared to all previous
        String hash4 = manifestHashProvider.calculateManifestHash(Arrays.asList(bundle1, bundle2), Collections.singleton(supportedOperation1));

        assertNotEquals(hash4, hash1);
        assertNotEquals(hash4, hash2);
        assertNotEquals(hash4, hash3);
    }
}