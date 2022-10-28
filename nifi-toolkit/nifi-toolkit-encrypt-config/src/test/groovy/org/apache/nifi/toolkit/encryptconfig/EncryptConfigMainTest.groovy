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
package org.apache.nifi.toolkit.encryptconfig

import org.junit.jupiter.api.Test

class EncryptConfigMainTest {

    @Test
    void testDetermineModeFromArgsWithLegacyMode() {
        def argsList = "-b conf/bootstrap.conf -n conf/nifi.properties".split(" ").toList()

        def toolMode = EncryptConfigMain.determineModeFromArgs(argsList)

        toolMode != null
        toolMode instanceof LegacyMode
    }

    @Test
    void testDetermineModeFromArgsWithNifiRegistryMode() {
        def argsList = "--nifiRegistry".split(" ").toList()

        def toolMode = EncryptConfigMain.determineModeFromArgs(argsList)

        toolMode != null
        toolMode instanceof NiFiRegistryMode
        !argsList.contains("--nifiRegistry")

        argsList = "-b conf/bootstrap.conf -p --nifiRegistry -r conf/nifi-registry.properties".split(" ").toList()

        toolMode = EncryptConfigMain.determineModeFromArgs(argsList)

        toolMode != null
        toolMode instanceof NiFiRegistryMode
        !argsList.contains("--nifiRegistry")
    }

    @Test
    void testDetermineModeFromArgsWithNifiRegistryDecryptMode() {
        def argsList = "--nifiRegistry --decrypt".split(" ").toList()

        def toolMode = EncryptConfigMain.determineModeFromArgs(argsList)

        toolMode != null
        toolMode instanceof NiFiRegistryDecryptMode
        !argsList.contains("--nifiRegistry")
        !argsList.contains("--decrypt")

        argsList = "--b conf/bootstrap.conf --decrypt --nifiRegistry".split(" ").toList()

        toolMode = EncryptConfigMain.determineModeFromArgs(argsList)

        toolMode != null
        toolMode instanceof NiFiRegistryDecryptMode
        !argsList.contains("--nifiRegistry")
        !argsList.contains("--decrypt")
    }

    @Test
    void testDetermineModeFromArgsReturnsNullOnDecryptWithoutNifiRegistryPresent() {
        def argsList = "--decrypt".split(" ").toList()

        def toolMode = EncryptConfigMain.determineModeFromArgs(argsList)

        toolMode == null
    }
}
