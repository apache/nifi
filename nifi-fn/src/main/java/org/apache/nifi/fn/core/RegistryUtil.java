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
package org.apache.nifi.fn.core;

import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.*;
import org.apache.nifi.util.NiFiProperties;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RegistryUtil {
    private RestBasedFlowRegistry registry;
    public RegistryUtil(String registryUrl){
        try {
            registry = new RestBasedFlowRegistry(new StandardFlowRegistryClient(),"id",registryUrl, SSLContext.getDefault(),"name");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
    public VersionedFlowSnapshot getFlowByName(String bucketName, String flowName) throws IOException, NiFiRegistryException {
        return getFlowByName(bucketName, flowName,-1);
    }
    public VersionedFlowSnapshot getFlowByName(String bucketName, String flowName, int versionID) throws IOException, NiFiRegistryException {

        //Get bucket by name
        Set<Bucket> buckets = this.getBuckets();
        Optional<Bucket> bucketOptional = buckets.stream().filter(b->b.getName().equals(bucketName)).findFirst();
        if(!bucketOptional.isPresent())
            throw new IllegalArgumentException("Bucket not found");
        String bucketID = bucketOptional.get().getIdentifier();


        //Get flow by name
        Set<VersionedFlow> flows = this.getFlows(bucketID);
        Optional<VersionedFlow> flowOptional = flows.stream().filter(b->b.getName().equals(flowName)).findFirst();
        if(!flowOptional.isPresent())
            throw new IllegalArgumentException("Flow not found");
        String flowID = flowOptional.get().getIdentifier();

        return getFlowByID(bucketID,flowID, versionID);
    }
    public VersionedFlowSnapshot getFlowByID(String bucketID, String flowID) throws IOException, NiFiRegistryException {
        return getFlowByID(bucketID, flowID,-1);
    }
    public VersionedFlowSnapshot getFlowByID(String bucketID, String flowID, int versionID) throws IOException, NiFiRegistryException {
        if(versionID == -1)
            versionID = this.getLatestVersion(bucketID, flowID);
        return registry.getFlowContents(bucketID, flowID, versionID,true);
    }

    public Map<String,String> getVariables(String bucketID, String flowID) throws IOException, NiFiRegistryException {
        VersionedFlowSnapshot flow = this.getFlowByID(bucketID,flowID);
        return flow.getFlowContents().getVariables();
    }
    public Map<String,String> getVariables(String bucketID, String flowID, int versionID) throws IOException, NiFiRegistryException {
        VersionedFlowSnapshot flow = this.getFlowByID(bucketID,flowID,versionID);
        return flow.getFlowContents().getVariables();
    }

    public Set<Bucket> getBuckets() throws IOException, NiFiRegistryException {
        return registry.getBuckets(NiFiUserUtils.getNiFiUser());
    }
    public Set<VersionedFlow> getFlows(String bucketID) throws IOException, NiFiRegistryException {
        return registry.getFlows(bucketID,NiFiUserUtils.getNiFiUser());
    }
    public int getLatestVersion(String bucketID, String flowID) throws IOException, NiFiRegistryException {
        return registry.getLatestVersion(bucketID, flowID, NiFiUserUtils.getNiFiUser());
    }
}
