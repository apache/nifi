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
import org.apache.nifi.toolkit.admin.client.ClientFactory
import org.junit.Rule
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.contrib.java.lang.system.SystemOutRule
import spock.lang.Specification

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
        config.notifyCluster(clientFactory,"src/test/resources/notify/conf/nifi.properties","src/test/resources/notify/conf/bootstrap.conf","/bogus/nifi/dir","shutting down in 30 seconds","WARN")

        then:

        1 * clientFactory.getClient(_,_) >> client
        1 * client.resource(_ as String) >> resource
        1 * resource.type(_) >> builder
        1 * builder.post(_,_) >> response
        1 * response.getStatus() >> 200

    }

    def "cluster message failed"(){

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse

        def config = new NotificationTool()

        when:
        config.notifyCluster(clientFactory,"src/test/resources/notify/conf/nifi.properties","src/test/resources/notify/conf/bootstrap.conf","/bogus/nifi/dir","shutting down in 30 seconds","WARN")

        then:

        1 * clientFactory.getClient(_,_) >> client
        1 * client.resource(_ as String) >> resource
        1 * resource.type(_) >> builder
        1 * builder.post(_,_) >> response
        1 * response.getStatus() >> 403
        def e = thrown(RuntimeException)
        e.message == "Failed with HTTP error code: 403"

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



}
