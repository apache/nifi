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

import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.web.api.entity.ClusterEntity
import org.junit.Rule
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.contrib.java.lang.system.SystemOutRule
import spock.lang.Specification

import javax.ws.rs.client.Client
import javax.ws.rs.client.Invocation
import javax.ws.rs.client.WebTarget
import javax.ws.rs.core.Response

class NiFiClientUtilSpec extends Specification{

    @Rule
    public ExpectedSystemExit exit = ExpectedSystemExit.none()

    @Rule
    public SystemOutRule systemOutRule = new SystemOutRule().enableLog()

    def "build unsecure url successfully"(){

        given:
        def NiFiProperties niFiProperties = Mock NiFiProperties


        when:
        def url = NiFiClientUtil.getUrl(niFiProperties,"/nifi-api/controller/cluster/nodes/1")

        then:

        2 * niFiProperties.getProperty(_) 
        niFiProperties.getProperty(NiFiProperties.WEB_HTTP_PORT) >> "8000"
        url == "http://localhost:8000/nifi-api/controller/cluster/nodes/1"
    }


    def "get cluster info successfully"(){

        given:
        def Client client = Mock Client
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def ClusterEntity clusterEntity = Mock ClusterEntity

        when:
        def entity = NiFiClientUtil.getCluster(client, niFiProperties, [], null)

        then:

        3 * niFiProperties.getProperty(_)
        1 * client.target(_ as String) >> resource
        1 * resource.request() >> builder
        1 * builder.get() >> response
        1 * response.getStatus() >> 200
        1 * response.readEntity(ClusterEntity.class) >> clusterEntity
        entity == clusterEntity

    }

    def "get secured cluster info successfully"(){

        given:
        def Client client = Mock Client
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def ClusterEntity clusterEntity = Mock ClusterEntity

        when:
        def entity = NiFiClientUtil.getCluster(client, niFiProperties, [], "ydavis@nifi")

        then:

        niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT) >> "8081"
        niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_HOST) >> "localhost"

        1 * client.target(_ as String) >> resource
        1 * resource.request() >> builder
        1 * builder.header(_,_) >> builder
        1 * builder.get() >> response
        1 * response.getStatus() >> 200
        1 * response.readEntity(ClusterEntity.class) >> clusterEntity
        entity == clusterEntity

    }


    def "get cluster info fails"(){

        given:
        def Client client = Mock Client
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def WebTarget resource = Mock WebTarget
        def Invocation.Builder builder = Mock Invocation.Builder
        def Response response = Mock Response
        def Response.StatusType statusType = Mock Response.StatusType

        when:

        NiFiClientUtil.getCluster(client, niFiProperties, [],null)

        then:

        3 * niFiProperties.getProperty(_)
        1 * client.target(_ as String) >> resource
        1 * resource.request() >> builder
        1 * builder.get() >> response
        1 * response.getStatus() >> 500
        1 * response.readEntity(String.class) >> "Only a node connected to a cluster can process the request."
        def e = thrown(RuntimeException)
        e.message == "Unable to obtain cluster information"

    }


}
