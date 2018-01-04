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

import org.junit.Ignore
import org.junit.Test

class EncryptConfigTest {

    @Ignore
    @Test
    void testMain() {

        String[] args = [];
        EncryptConfigMain.main(args)

    }

    @Ignore
    @Test
    void testNiFiRegistryModeMain() {

        String[] args = [
                "--nifiRegistry",
                "-b", "/Users/kdoran/dev/nr-ec-test-conf/bootstrap.conf",
                "-k", "0123456789ABCDEFFEDCBA9876543210",
                //"-r", "/Users/kdoran/dev/nr-ec-test-conf/nifi-registry.properties",
                "-i", "/Users/kdoran/dev/nr-ec-test-conf/identity-providers.xml",
                "-a", "/Users/kdoran/dev/nr-ec-test-conf/authorizers.xml",
        ];
        EncryptConfigMain.main(args)

    }

    //@Ignore
    @Test
    void testDecryptModeMain() {

        String[] args = [
                "--nifiRegistry", "--decrypt", "-v",
                //"-b", "/Users/kdoran/dev/nifi-toolkit/test-data/encrypt-config/conf/bootstrap.conf",
                //"-k", "0123456789ABCDEFFEDCBA9876543210",
                "-p",
                //"-t", "properties",
                "-r", "/Users/kdoran/dev/nifi-toolkit/test-data/encrypt-config/conf/nifi-registry.properties",
                //"/Users/kdoran/dev/nifi-toolkit/test-data/encrypt-config/conf/authorizers.xml",
        ];
        EncryptConfigMain.main(args)

    }

}
