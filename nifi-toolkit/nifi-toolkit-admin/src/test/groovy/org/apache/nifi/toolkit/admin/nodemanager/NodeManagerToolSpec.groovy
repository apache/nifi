
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
package org.apache.nifi.toolkit.admin.nodemanager

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.WebResource
import org.apache.commons.cli.ParseException
import org.apache.nifi.toolkit.admin.client.ClientFactory
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.web.api.dto.ClusterDTO
import org.apache.nifi.web.api.dto.NodeDTO
import org.apache.nifi.web.api.entity.ClusterEntity
import org.apache.nifi.web.api.entity.NodeEntity
import org.junit.Rule
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.contrib.java.lang.system.SystemOutRule
import spock.lang.Specification

class NodeManagerToolSpec extends Specification{

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none()

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog()


    def "print help and usage info"() {

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def config = new NodeManagerTool()

        when:
        config.parse(clientFactory,["-h"] as String[])

        then:
        systemOutRule.getLog().contains("usage: org.apache.nifi.toolkit.admin.nodemanager.NodeManagerTool")
    }

    def "throws exception missing bootstrap conf flag"() {

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def config = new NodeManagerTool()

        when:
        config.parse(clientFactory,["-d", "/install/nifi"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -b option"
    }

    def "throws exception missing directory"(){

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def config = new NodeManagerTool()

        when:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf/bootstrap.conf"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -d option"
    }

    def "throws exception missing operation"(){

        given:
        def ClientFactory clientFactory = Mock ClientFactory
        def config = new NodeManagerTool()

        when:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf/bootstrap.conf","-d", "/install/nifi"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -o option"
    }

    def "throws exception invalid operation"(){

        given:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "CONNECTED"
        nodeDTO.apiPort = 8080
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        clientFactory.getClient(_,_) >> client
        client.resource(_ as String) >> resource
        resource.type(_) >> builder
        builder.get(ClientResponse.class) >> response
        builder.put(_,_) >> response
        builder.delete(ClientResponse.class,_) >> response
        response.getStatus() >> 200
        response.getEntity(ClusterEntity.class) >> clusterEntity
        response.getEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        when:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf/bootstrap.conf","-d","/install/nifi","-o","fake"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Invalid operation provided: fake"
    }

    def "get node info successfully"(){

        given:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "1"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def config = new NodeManagerTool()

        when:
        def entity = config.getCurrentNode(clusterEntity,niFiProperties)

        then:

        1 * clusterEntity.getCluster() >> clusterDTO
        1 * clusterDTO.getNodes() >> nodeDTOs
        2 * niFiProperties.getProperty(_) >> "1"
        entity == nodeDTO

    }

    def "delete node successfully"(){

        given:
        def String url = "http://locahost:8080/nifi-api/controller"
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse
        def config = new NodeManagerTool()

        when:
        config.deleteNode(url,client)

        then:

        1 * client.resource(_ as String) >> resource
        1 * resource.type(_) >> builder
        1 * builder.delete(_) >> response
        1 * response.getStatus() >> 200

    }

    def "delete node failed"(){

        given:
        def String url = "http://locahost:8080/nifi-api/controller"
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse
        def config = new NodeManagerTool()

        when:
        config.deleteNode(url,client)

        then:
        1 * client.resource(_ as String) >> resource
        1 * resource.type(_) >> builder
        1 * builder.delete(_) >> response
        2 * response.getStatus() >> 403
        def e = thrown(RuntimeException)
        e.message == "Failed with HTTP error code: 403"

    }

    def "update node successfully"(){

        given:
        def String url = "http://locahost:8080/nifi-api/controller"
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse
        def NodeDTO nodeDTO = new NodeDTO()
        def NodeEntity nodeEntity = Mock NodeEntity
        def config = new NodeManagerTool()

        when:
        def entity = config.updateNode(url,client,nodeDTO,NodeManagerTool.STATUS.DISCONNECTING)

        then:
        1 * client.resource(_ as String) >> resource
        1 * resource.type(_) >> builder
        1 * builder.put(_,_) >> response
        1 * response.getStatus() >> 200
        1 * response.getEntity(NodeEntity.class) >> nodeEntity
        entity == nodeEntity

    }

    def "update node fails"(){

        given:
        def String url = "http://locahost:8080/nifi-api/controller"
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse
        def NodeDTO nodeDTO = new NodeDTO()
        def config = new NodeManagerTool()

        when:
        config.updateNode(url,client,nodeDTO,NodeManagerTool.STATUS.DISCONNECTING)

        then:
        1 * client.resource(_ as String) >> resource
        1 * resource.type(_) >> builder
        1 * builder.put(_,_) >> response
        2 * response.getStatus() >> 403
        def e = thrown(RuntimeException)
        e.message == "Failed with HTTP error code: 403"

    }

    def "disconnect node successfully"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "CONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        client.resource(_ as String) >> resource
        resource.type(_) >> builder
        builder.get(ClientResponse.class) >> response
        builder.put(_,_) >> response
        response.getStatus() >> 200
        response.getEntity(ClusterEntity.class) >> clusterEntity
        response.getEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        expect:
        config.disconnectNode(client, niFiProperties,["http://localhost:8080"])

    }

    def "connect node successfully"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "DISCONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        client.resource(_ as String) >> resource
        resource.type(_) >> builder
        builder.get(ClientResponse.class) >> response
        builder.put(_,_) >> response
        response.getStatus() >> 200
        response.getEntity(ClusterEntity.class) >> clusterEntity
        response.getEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        expect:
        config.connectNode(client, niFiProperties,["http://localhost:8080"])

    }

    def "remove node successfully"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "CONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        client.resource(_ as String) >> resource
        resource.type(_) >> builder
        builder.get(ClientResponse.class) >> response
        builder.put(_,_) >> response
        builder.delete(ClientResponse.class,_) >> response
        response.getStatus() >> 200
        response.getEntity(ClusterEntity.class) >> clusterEntity
        response.getEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"

        expect:
        config.removeNode(client, niFiProperties,["http://localhost:8080"])

    }

    def "parse args and delete node"(){

        setup:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def ClientFactory clientFactory = Mock ClientFactory
        def Client client = Mock Client
        def WebResource resource = Mock WebResource
        def WebResource.Builder builder = Mock WebResource.Builder
        def ClientResponse response = Mock ClientResponse
        def ClusterEntity clusterEntity = Mock ClusterEntity
        def ClusterDTO clusterDTO = Mock ClusterDTO
        def NodeDTO nodeDTO = new NodeDTO()
        nodeDTO.address = "localhost"
        nodeDTO.nodeId = "1"
        nodeDTO.status = "CONNECTED"
        def List<NodeDTO> nodeDTOs = [nodeDTO]
        def NodeEntity nodeEntity = new NodeEntity()
        nodeEntity.node = nodeDTO
        def config = new NodeManagerTool()


        niFiProperties.getProperty(_) >> "localhost"
        clientFactory.getClient(_,_) >> client
        client.resource(_ as String) >> resource
        resource.type(_) >> builder
        builder.get(ClientResponse.class) >> response
        builder.put(_,_) >> response
        builder.delete(ClientResponse.class,_) >> response
        response.getStatus() >> 200
        response.getEntity(ClusterEntity.class) >> clusterEntity
        response.getEntity(NodeEntity.class) >> nodeEntity
        clusterEntity.getCluster() >> clusterDTO
        clusterDTO.getNodes() >> nodeDTOs
        nodeDTO.address >> "localhost"


        expect:
        config.parse(clientFactory,["-b","src/test/resources/notify/conf/bootstrap.conf","-d","/bogus/nifi/dir","-o","remove","-u","http://localhost:8080,http://localhost1:8080"] as String[])

    }


}
