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

package org.apache.nifi.toolkit.admin.client

import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.properties.NiFiPropertiesLoader
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandalone
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandaloneCommandLine
import org.apache.nifi.util.NiFiProperties
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission

class NiFiClientFactorySpec extends Specification {

    def "get client for unsecure nifi"(){

        given:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def clientFactory = new NiFiClientFactory()

        when:
        def client = clientFactory.getClient(niFiProperties,"src/test/resources/notify")

        then:
        client

    }

    def "get client for secured nifi"(){

        given:
        def File tmpDir = setupTmpDir()
        def File testDir = new File("target/tmp/keys")
        def toolkitCommandLine = ["-O", "-o",testDir.absolutePath,"-n","localhost","-C", "CN=user1","-S", "badKeyPass", "-K", "badKeyPass", "-P", "badTrustPass"]

        TlsToolkitStandaloneCommandLine tlsToolkitStandaloneCommandLine = new TlsToolkitStandaloneCommandLine()
        tlsToolkitStandaloneCommandLine.parse(toolkitCommandLine as String[])
        new TlsToolkitStandalone().createNifiKeystoresAndTrustStores(tlsToolkitStandaloneCommandLine.createConfig())

        def bootstrapConfFile = "src/test/resources/notify/conf/bootstrap.conf"
        def nifiPropertiesFile = "src/test/resources/notify/conf/nifi-secured.properties"
        def key = NiFiPropertiesLoader.extractKeyFromBootstrapFile(bootstrapConfFile)
        def NiFiProperties niFiProperties = NiFiPropertiesLoader.withKey(key).load(nifiPropertiesFile)
        def clientFactory = new NiFiClientFactory()

        when:
        def client = clientFactory.getClient(niFiProperties,"src/test/resources/notify")

        then:
        client

        cleanup:
        tmpDir.deleteDir()

    }

    def setFilePermissions(File file, List<PosixFilePermission> permissions = []) {
        if (SystemUtils.IS_OS_WINDOWS) {
            file?.setReadable(permissions.contains(PosixFilePermission.OWNER_READ))
            file?.setWritable(permissions.contains(PosixFilePermission.OWNER_WRITE))
            file?.setExecutable(permissions.contains(PosixFilePermission.OWNER_EXECUTE))
        } else {
            Files.setPosixFilePermissions(file?.toPath(), permissions as Set)
        }
    }
    def setupTmpDir(String tmpDirPath = "target/tmp/") {
        File tmpDir = new File(tmpDirPath)
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
                                    PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
                                    PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])
        tmpDir
    }

}
