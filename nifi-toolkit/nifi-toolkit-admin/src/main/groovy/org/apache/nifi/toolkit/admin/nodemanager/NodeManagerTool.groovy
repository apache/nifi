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
import org.apache.nifi.toolkit.admin.AbstractAdminTool
import org.apache.nifi.toolkit.admin.client.NiFiClientUtil
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.nifi.properties.NiFiPropertiesLoader
import org.apache.nifi.toolkit.admin.client.ClientFactory
import org.apache.nifi.toolkit.admin.client.NiFiClientFactory
import org.apache.nifi.toolkit.admin.util.AdminUtil
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.util.StringUtils
import org.apache.nifi.web.api.dto.NodeDTO
import org.apache.nifi.web.api.entity.ClusterEntity
import org.apache.nifi.web.api.entity.NodeEntity
import org.apache.nifi.web.security.ProxiedEntitiesUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Paths

public class NodeManagerTool extends AbstractAdminTool {

    private static final String DEFAULT_DESCRIPTION = "This tool is used to manage nodes within a cluster. Supported functionality will remove node from cluster. "
    private static final String HELP_ARG = "help"
    private static final String VERBOSE_ARG = "verbose"
    private static final String PROXY_DN = "proxyDn"
    private static final String BOOTSTRAP_CONF = "bootstrapConf"
    private static final String NIFI_INSTALL_DIR = "nifiInstallDir"
    private static final String CLUSTER_URLS = "clusterUrls"
    private static final String REMOVE = "remove"
    private static final String DISCONNECT = "disconnect"
    private static final String CONNECT = "connect"
    private static final String NODE_STATUS = "status"
    private static final String OPERATION = "operation"
    private final static String NODE_ENDPOINT = "/nifi-api/controller/cluster/nodes"
    private final static String NIFI_ENDPOINT = "/nifi"
    private final static String SUPPORTED_MINIMUM_VERSION = "1.0.0"
    static enum STATUS {DISCONNECTING,CONNECTING,CONNECTED}

    NodeManagerTool() {
        header = buildHeader(DEFAULT_DESCRIPTION)
        setup()
    }

    NodeManagerTool(final String description){
        this.header = buildHeader(description)
        setup()
    }

    @Override
    protected Logger getLogger() {
        LoggerFactory.getLogger(NodeManagerTool)
    }

    protected Options getOptions(){
        final Options options = new Options()
        options.addOption(Option.builder("h").longOpt(HELP_ARG).desc("Print help info").build())
        options.addOption(Option.builder("v").longOpt(VERBOSE_ARG).desc("Set mode to verbose (default is false)").build())
        options.addOption(Option.builder("p").longOpt(PROXY_DN).hasArg().desc("User or Proxy DN that has permission to send a notification. User must have view and modify privileges to 'access the controller' in NiFi").build())
        options.addOption(Option.builder("b").longOpt(BOOTSTRAP_CONF).hasArg().desc("Existing Bootstrap Configuration file").build())
        options.addOption(Option.builder("d").longOpt(NIFI_INSTALL_DIR).hasArg().desc("NiFi Installation Directory").build())
        options.addOption(Option.builder("o").longOpt(OPERATION).hasArg().desc("Operations supported: status, connect (cluster), disconnect(cluster), remove (cluster)").build())
        options.addOption(Option.builder("u").longOpt(CLUSTER_URLS).hasArg().desc("List of active urls for the cluster").build())
        options
    }

    NodeDTO getCurrentNode(ClusterEntity clusterEntity, NiFiProperties niFiProperties){
        final List<NodeDTO> nodeDTOs = clusterEntity.cluster.nodes
        final String nodeHost = StringUtils.isEmpty(niFiProperties.getProperty(NiFiProperties.CLUSTER_NODE_ADDRESS)) ?
                "localhost":niFiProperties.getProperty(NiFiProperties.CLUSTER_NODE_ADDRESS)
        return nodeDTOs.find{ it.address == nodeHost }
    }

    NodeEntity updateNode(final String url, final Client client, final NodeDTO nodeDTO, final STATUS nodeStatus,final String proxyDN){
        final WebResource webResource = client.resource(url)
        nodeDTO.status = nodeStatus
        String json = NiFiClientUtil.convertToJson(nodeDTO)

        if(isVerbose){
            logger.info("Sending node info for update: " + json)
        }

        ClientResponse response

        if(url.startsWith("https")) {
            response = webResource.type("application/json").header(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, ProxiedEntitiesUtils.formatProxyDn(proxyDN)).put(ClientResponse.class, json)
        }else{
            response = webResource.type("application/json").put(ClientResponse.class, json)
        }

        if(response.status != 200){
            throw new RuntimeException("Failed with HTTP error code " + response.status + " with reason: " +response.getEntity(String.class))
        }else{
            response.getEntity(NodeEntity.class)
        }
    }

    void deleteNode(final String url, final Client client, final String proxyDN){
        final WebResource webResource = client.resource(url)

        if(isVerbose){
            logger.info("Attempting to delete node" )
        }
        ClientResponse response

        if(url.startsWith("https")) {
            response = webResource.type("application/json").header(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, ProxiedEntitiesUtils.formatProxyDn(proxyDN)).delete(ClientResponse.class)
        }else{
            response = webResource.type("application/json").delete(ClientResponse.class)
        }

        if(response.status != 200){
            throw new RuntimeException("Failed with HTTP error code " + response.status + " with reason: " +response.getEntity(String.class))
        }
    }

    void getStatus(final Client client,NiFiProperties niFiProperties,List<String> activeUrls){
        if(activeUrls == null || activeUrls.empty) {
            final String nodeUrl = NiFiClientUtil.getUrl(niFiProperties, null)
            activeUrls = [nodeUrl]
        }

        for(String activeUrl: activeUrls) {
            final String url = activeUrl + NIFI_ENDPOINT
            final WebResource webResource = client.resource(url)

            if (isVerbose) {
                logger.info("Checking if node is available")
            }

            try {
                final ClientResponse response = webResource.get(ClientResponse.class)
                if (response.status == 200) {
                    System.out.println("NiFi Node is running and available at "+url)
                } else {
                    System.out.println("Attempt to contact NiFi Node at "+url+" returned Response Code: " + response.status + " with reason: " + response.getEntity(String.class))
                }
            } catch (Exception ex) {
                System.out.println("Attempt to contact NiFi Node "+url+" did not complete due to exception: " + ex.localizedMessage)
            }
        }

    }

    void disconnectNode(final Client client, NiFiProperties niFiProperties, List<String> activeUrls, final String proxyDN){
        final ClusterEntity clusterEntity = NiFiClientUtil.getCluster(client, niFiProperties, activeUrls,proxyDN)
        NodeDTO currentNode = getCurrentNode(clusterEntity,niFiProperties)
        if(currentNode != null){
            for(String activeUrl: activeUrls) {
                try {
                    final String url = activeUrl + NODE_ENDPOINT + File.separator + currentNode.nodeId
                    updateNode(url, client, currentNode, STATUS.DISCONNECTING,proxyDN)
                    return
                } catch (Exception ex){
                    logger.warn("Could not connect to node on "+activeUrl+". Exception: "+ex.toString())
                }
            }
            throw new RuntimeException("Could not successfully complete request")
        }else{
            throw new RuntimeException("Current node could not be found in the cluster")
        }
    }

    void connectNode(final Client client, NiFiProperties niFiProperties,List<String> activeUrls, final String proxyDN){
        final ClusterEntity clusterEntity = NiFiClientUtil.getCluster(client, niFiProperties, activeUrls,proxyDN)
        NodeDTO currentNode = getCurrentNode(clusterEntity,niFiProperties)

        if(currentNode != null) {
            for(String activeUrl: activeUrls) {
                try {
                    final String url = activeUrl + NODE_ENDPOINT + File.separator + currentNode.nodeId
                    updateNode(url, client, currentNode, STATUS.CONNECTING,proxyDN)
                    return
                } catch (Exception ex){
                    logger.warn("Could not connect to node on "+activeUrl+". Exception: "+ex.toString())
                }
            }
            throw new RuntimeException("Could not successfully complete request")
        }else{
            throw new RuntimeException("Current node could not be found in the cluster")
        }
    }

    void removeNode(final Client client, NiFiProperties niFiProperties, List<String> activeUrls, final String proxyDN){

        final ClusterEntity clusterEntity = NiFiClientUtil.getCluster(client, niFiProperties, activeUrls,proxyDN)
        NodeDTO currentNode = getCurrentNode(clusterEntity,niFiProperties)

        if(currentNode != null) {

            for (String activeUrl : activeUrls) {

                try {

                    final String url = activeUrl + NODE_ENDPOINT + File.separator + currentNode.nodeId

                    if(isVerbose){
                        logger.info("Attempting to connect to cluster with url:" + url)
                    }

                    if(currentNode.status == "CONNECTED") {
                        currentNode = updateNode(url, client, currentNode, STATUS.DISCONNECTING,proxyDN).node
                    }

                    if(currentNode.status == "DISCONNECTED") {
                        deleteNode(url, client,proxyDN)
                    }

                    if(isVerbose){
                        logger.info("Node removed from cluster successfully.")
                    }

                    return

                }catch (Exception ex){
                    logger.warn("Could not connect to node on "+activeUrl+". Exception: "+ex.toString())
                }

            }
            throw new RuntimeException("Could not successfully complete request")

        }else{
            throw new RuntimeException("Current node could not be found in the cluster")
        }

    }

    void parse(final ClientFactory clientFactory, final String[] args) throws ParseException, UnsupportedOperationException, IllegalArgumentException {

        final CommandLine commandLine = new DefaultParser().parse(options,args)

        if (commandLine.hasOption(HELP_ARG)){
            printUsage(null)
        }else{

            if(commandLine.hasOption(BOOTSTRAP_CONF) && commandLine.hasOption(NIFI_INSTALL_DIR) && commandLine.hasOption(OPERATION)) {

                if(commandLine.hasOption(VERBOSE_ARG)){
                    this.isVerbose = true
                }

                final String bootstrapConfFileName = commandLine.getOptionValue(BOOTSTRAP_CONF)
                final String proxyDN = commandLine.getOptionValue(PROXY_DN)
                final File bootstrapConf = new File(bootstrapConfFileName)
                Properties bootstrapProperties = AdminUtil.getBootstrapConf(Paths.get(bootstrapConfFileName))
                String nifiConfDir = AdminUtil.getRelativeDirectory(bootstrapProperties.getProperty("conf.dir"), bootstrapConf.getCanonicalFile().getParentFile().getParentFile().getCanonicalPath())
                String nifiLibDir = AdminUtil.getRelativeDirectory(bootstrapProperties.getProperty("lib.dir"), bootstrapConf.getCanonicalFile().getParentFile().getParentFile().getCanonicalPath())
                String nifiPropertiesFileName = nifiConfDir + File.separator +"nifi.properties"
                final String key = NiFiPropertiesLoader.extractKeyFromBootstrapFile(bootstrapConfFileName)
                final NiFiProperties niFiProperties = NiFiPropertiesLoader.withKey(key).load(nifiPropertiesFileName)
                final String operation = commandLine.getOptionValue(OPERATION)

                if(!StringUtils.isEmpty(niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT)) && StringUtils.isEmpty(proxyDN) && !operation.equalsIgnoreCase(NODE_STATUS)) {
                    throw new UnsupportedOperationException("Proxy DN is required for sending a notification to this node or cluster")
                }

                final String nifiInstallDir = commandLine.getOptionValue(NIFI_INSTALL_DIR)

                if(AdminUtil.supportedNiFiMinimumVersion(nifiConfDir,nifiLibDir,SUPPORTED_MINIMUM_VERSION)){

                    final Client client = clientFactory.getClient(niFiProperties,nifiInstallDir)

                    if(isVerbose){
                        logger.info("Starting {} request",operation)
                    }

                    List<String> activeUrls = null
                    if (commandLine.hasOption(CLUSTER_URLS)) {
                        final String urlList = commandLine.getOptionValue(CLUSTER_URLS)
                        activeUrls = urlList.tokenize(',')
                    }

                    if(operation.equalsIgnoreCase(NODE_STATUS)){
                        getStatus(client,niFiProperties,activeUrls)
                    }else{

                        if(NiFiClientUtil.isCluster(niFiProperties)) {

                            if (activeUrls == null) {
                                activeUrls = NiFiClientUtil.getActiveClusterUrls(client, niFiProperties, proxyDN)
                            }

                            if (isVerbose) {
                                logger.info("Using active urls {} for communication.", activeUrls)
                            }

                            if (operation.toLowerCase().equals(REMOVE)) {
                                removeNode(client, niFiProperties, activeUrls, proxyDN)
                            } else if (operation.toLowerCase().equals(DISCONNECT)) {
                                disconnectNode(client, niFiProperties, activeUrls, proxyDN)
                            } else if (operation.toLowerCase().equals(CONNECT)) {
                                connectNode(client, niFiProperties, activeUrls, proxyDN)
                            } else {
                                throw new ParseException("Invalid operation provided: " + operation)
                            }
                        }else{
                            throw new UnsupportedOperationException("The provided operation ("+operation+") is only supported with instances of NiFi running within a cluster.")
                        }

                    }


                }else{
                    throw new UnsupportedOperationException("Node Manager Tool only supports instances of NiFi running versions 1.0.0 or higher.")
                }

            }else if(!commandLine.hasOption(BOOTSTRAP_CONF)){
                throw new ParseException("Missing -b option")
            }else if(!commandLine.hasOption(NIFI_INSTALL_DIR)){
                throw new ParseException("Missing -d option")
            }else{
                throw new ParseException("Missing -o option")
            }
        }

    }

    public static void main(String[] args) {
        final NodeManagerTool tool = new NodeManagerTool()
        final ClientFactory clientFactory = new NiFiClientFactory()

        try{
            tool.parse(clientFactory,args)
        } catch (Exception e ) {
            tool.printUsage(e.getLocalizedMessage())
            System.exit(1)
        }

        System.exit(0)
    }



}
