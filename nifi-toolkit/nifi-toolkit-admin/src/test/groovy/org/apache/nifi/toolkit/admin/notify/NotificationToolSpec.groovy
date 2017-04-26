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

package org.apache.nifi.toolkit.admin.notify

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.WebResource
import org.apache.commons.cli.ParseException
import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.toolkit.admin.client.ClientFactory
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandalone
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandaloneCommandLine
import org.junit.Rule
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.contrib.java.lang.system.SystemOutRule
import spock.lang.Specification

import javax.ws.rs.core.Response
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission

class NotificationToolSpec extends Specification{

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none()

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog()


    def "print help and usage info"() {

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def config = new NotificationTool()

        when:
        config.parse(clientFactory,["-h"] as String[])

        then:
        systemOutRule.getLog().contains("usage: org.apache.nifi.toolkit.admin.notify.NotificationTool")
    }

    def "throws exception missing bootstrap conf flag"() {

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def config = new NotificationTool()

        when:
        config.parse(clientFactory,["-d", "/missing/bootstrap/conf"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -b option"
    }

    def "throws exception missing message"(){

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def config = new NotificationTool()

        when:
        config.parse(clientFactory,["-b","/tmp/fake/upgrade/conf","-v","-d","/bogus/nifi/dir"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -m option"
    }

    def "throws exception missing directory"(){

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def config = new NotificationTool()

        when:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf/bootstrap.conf","-m","shutting down in 30 seconds"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -d option"
    }


    def "send cluster message successfully"(){

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse

        def config = new NotificationTool()

        when:
        config.notifyCluster(clientFactory,"src/test/resources/notify/conf/nifi.properties","src/test/resources/notify/conf/bootstrap.conf","/bogus/nifi/dir","shutting down in 30 seconds","WARN",null)

        then:

        1 * clientFactory.getClient(_,_) >> client
        1 * client.resource(_ as String) >> resource
        1 * resource.type(_) >> builder
        1 * builder.post(_,_) >> response
        1 * response.getStatus() >> 200

    }

    def "send secured cluster cluster message successfully"(){

        given:

        def File tmpDir = setupTmpDir()
        def File testDir = new File("target/tmp/keys")
        def toolkitCommandLine = ["-O", "-o",testDir.absolutePath,"-n","localhost","-C", "CN=user1","-S", "badKeyPass", "-K", "badKeyPass", "-P", "badTrustPass"]

        TlsToolkitStandaloneCommandLine tlsToolkitStandaloneCommandLine = new TlsToolkitStandaloneCommandLine()
        tlsToolkitStandaloneCommandLine.parse(toolkitCommandLine as String[])
        new TlsToolkitStandalone().createNifiKeystoresAndTrustStores(tlsToolkitStandaloneCommandLine.createConfig())

        def bootstrapConfFile = "src/test/resources/notify/conf/bootstrap.conf"
        def nifiPropertiesFile = "src/test/resources/notify/conf/nifi-secured.properties"

        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse

        def config = new NotificationTool()

        when:
        config.notifyCluster(clientFactory,nifiPropertiesFile,bootstrapConfFile,"/bogus/nifi/dir","shutting down in 30 seconds","WARN","ydavis@nifi")

        then:

        1 * clientFactory.getClient(_,_) >> client
        1 * client.resource(_ as String) >> resource
        1 * resource.type(_) >> builder
        1 * builder.header(_,_) >> builder
        1 * builder.post(_,_) >> response
        1 * response.getStatus() >> 200

        cleanup:
        tmpDir.deleteDir()

    }



    def "cluster message failed"(){

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse
        def Response.StatusType statusType = Mock Response.StatusType

        def config = new NotificationTool()

        when:
        config.notifyCluster(clientFactory,"src/test/resources/notify/conf/nifi.properties","src/test/resources/notify/conf/bootstrap.conf","/bogus/nifi/dir","shutting down in 30 seconds","WARN","ydavis@nifi")

        then:

        1 * clientFactory.getClient(_,_) >> client
        1 * client.resource(_ as String) >> resource
        1 * resource.type(_) >> builder
        1 * builder.post(_,_) >> response
        1 * response.getStatus() >> 403
        1 * response.getEntity(String.class) >> "Unauthorized User"
        def e = thrown(RuntimeException)
        e.message == "Failed with HTTP error code 403 with reason: Unauthorized User"

    }

    def "send secured cluster cluster message fails due to missing proxy dn"(){

        given:

        def File tmpDir = setupTmpDir()
        def File testDir = new File("target/tmp/keys")
        def toolkitCommandLine = ["-O", "-o",testDir.absolutePath,"-n","localhost","-C", "CN=user1","-S", "badKeyPass", "-K", "badKeyPass", "-P", "badTrustPass"]

        TlsToolkitStandaloneCommandLine tlsToolkitStandaloneCommandLine = new TlsToolkitStandaloneCommandLine()
        tlsToolkitStandaloneCommandLine.parse(toolkitCommandLine as String[])
        new TlsToolkitStandalone().createNifiKeystoresAndTrustStores(tlsToolkitStandaloneCommandLine.createConfig())

        def bootstrapConfFile = "src/test/resources/notify/conf/bootstrap.conf"
        def nifiPropertiesFile = "src/test/resources/notify/conf/nifi-secured.properties"

        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse

        def config = new NotificationTool()

        when:
        config.notifyCluster(clientFactory,nifiPropertiesFile,bootstrapConfFile,"/bogus/nifi/dir","shutting down in 30 seconds","WARN",null)

        then:

        1 * clientFactory.getClient(_,_) >> client
        1 * client.resource(_ as String) >> resource
        def e = thrown(UnsupportedOperationException)
        e.message == "Proxy DN is required for sending a notification to this node or cluster"

        cleanup:
        tmpDir.deleteDir()

    }

    def "parse comment and send cluster message successfully"(){

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse

        def config = new NotificationTool()

        when:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf/bootstrap.conf","-d","/bogus/nifi/dir","-m","shutting down in 30 seconds","-l","ERROR"] as String[])

        then:

        1 * clientFactory.getClient(_,_) >> client
        1 * client.resource(_ as String) >> resource
        1 * resource.type(_) >> builder
        1 * builder.post(_,_) >> response
        1 * response.getStatus() >> 200

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
