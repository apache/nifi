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

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Lists
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.util.StringUtils
import org.apache.nifi.web.api.dto.NodeDTO
import org.apache.nifi.web.api.dto.util.DateTimeAdapter
import org.apache.nifi.web.api.entity.ClusterEntity
import org.apache.nifi.web.api.entity.NodeEntity
import org.apache.nifi.web.security.ProxiedEntitiesUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.ws.rs.client.Client
import javax.ws.rs.client.WebTarget
import javax.ws.rs.core.Response
import java.text.SimpleDateFormat

public class NiFiClientUtil {

    private static final Logger logger = LoggerFactory.getLogger(NiFiClientUtil.class)
    private final static String GET_CLUSTER_ENDPOINT ="/nifi-api/controller/cluster"

    public static Boolean isCluster(final NiFiProperties niFiProperties){
        String clusterNode = niFiProperties.getProperty(NiFiProperties.CLUSTER_IS_NODE)
        return Boolean.valueOf(clusterNode)
    }

    public static String getUrl(NiFiProperties niFiProperties, String endpoint){

        final StringBuilder urlBuilder = new StringBuilder();

        if(!StringUtils.isEmpty(niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT))){
            urlBuilder.append("https://")
            urlBuilder.append(StringUtils.isEmpty(niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_HOST)) ? "localhost": niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_HOST))
            urlBuilder.append(":")
            urlBuilder.append(StringUtils.isEmpty(niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT)) ? "8081" : niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT))
        }else{
            urlBuilder.append("http://")
            urlBuilder.append(StringUtils.isEmpty(niFiProperties.getProperty(NiFiProperties.WEB_HTTP_HOST)) ? "localhost": niFiProperties.getProperty(NiFiProperties.WEB_HTTP_HOST))
            urlBuilder.append(":")
            urlBuilder.append(StringUtils.isEmpty(niFiProperties.getProperty(NiFiProperties.WEB_HTTP_PORT)) ? "8080": niFiProperties.getProperty(NiFiProperties.WEB_HTTP_PORT))
        }

        if(!StringUtils.isEmpty(endpoint)) {
            urlBuilder.append(endpoint)
        }

        urlBuilder.toString()
    }

    public static String getUrl(NiFiProperties niFiProperties, NodeDTO nodeDTO, String endpoint){

        final StringBuilder urlBuilder = new StringBuilder();
        if(!StringUtils.isEmpty(niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT))){
            urlBuilder.append("https://")

        }else{
            urlBuilder.append("http://")
        }
        urlBuilder.append(nodeDTO.address)
        urlBuilder.append(":")
        urlBuilder.append(nodeDTO.apiPort)

        if(!StringUtils.isEmpty(endpoint)) {
            urlBuilder.append(endpoint)
        }

        urlBuilder.toString()
    }

    public static ClusterEntity getCluster(final Client client, NiFiProperties niFiProperties, List<String> activeUrls, final String proxyDN){

        if(activeUrls.isEmpty()){
            final String url = getUrl(niFiProperties,null)
            activeUrls.add(url)
        }

        for(String activeUrl: activeUrls) {

            try {

                String url = activeUrl + GET_CLUSTER_ENDPOINT
                final WebTarget webTarget = client.target(url)
                Response response

                if(url.startsWith("https")) {
                    response = webTarget.request().header(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, ProxiedEntitiesUtils.formatProxyDn(proxyDN)).get()
                }else{
                    response = webTarget.request().get()
                }

                Integer status = response.status

                if (status != 200) {
                    if (status == 404) {
                        logger.warn("This node is not attached to a cluster. Please connect to a node that is attached to the cluster for information")
                    } else {
                        logger.warn("Failed with HTTP error code: {}, message: {}", status, response.readEntity(String.class))
                    }
                } else if (status == 200) {
                    return response.readEntity(ClusterEntity.class)
                }

            }catch(Exception ex){
                logger.warn("Exception occurred during connection attempt: {}",ex.localizedMessage)
            }

        }

        throw new RuntimeException("Unable to obtain cluster information")

    }

    public static List<String> getActiveClusterUrls(final Client client, NiFiProperties niFiProperties, final String proxyDN){

        final ClusterEntity clusterEntity = getCluster(client, niFiProperties, Lists.newArrayList(),proxyDN)
        final List<NodeDTO> activeNodes = clusterEntity.cluster.nodes.findAll{ it.status == "CONNECTED" }
        final List<String> activeUrls = Lists.newArrayList()

        activeNodes.each {
            activeUrls.add(getUrl(niFiProperties,it, null))
        }
        activeUrls
    }

    public static String convertToJson(NodeDTO nodeDTO){
        ObjectMapper om = new ObjectMapper()
        om.setDateFormat(new SimpleDateFormat(DateTimeAdapter.DEFAULT_DATE_TIME_FORMAT));
        NodeEntity ne = new NodeEntity()
        ne.setNode(nodeDTO)
        return om.writeValueAsString(ne)
    }
}
