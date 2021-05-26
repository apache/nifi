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
package org.apache.nifi.registry.authorization;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel
public class ResourcePermissions {

    private Permissions buckets = new Permissions();
    private Permissions tenants = new Permissions();
    private Permissions policies = new Permissions();
    private Permissions proxy = new Permissions();

    @ApiModelProperty(
            value = "The access that the current user has to any top level resources (a logical 'OR' of all other values)",
            readOnly = true)
    public Permissions getAnyTopLevelResource() {
        return new Permissions()
                .withCanRead(buckets.getCanRead()
                        || tenants.getCanRead()
                        || policies.getCanRead()
                        || proxy.getCanRead())
                .withCanWrite(buckets.getCanWrite()
                        || tenants.getCanWrite()
                        || policies.getCanWrite()
                        || proxy.getCanWrite())
                .withCanDelete(buckets.getCanDelete()
                        || tenants.getCanDelete()
                        || policies.getCanDelete()
                        || proxy.getCanDelete());
    }

    @ApiModelProperty(
            value = "The access that the current user has to the top level /buckets resource of this NiFi Registry (i.e., access to all buckets)",
            readOnly = true)
    public Permissions getBuckets() {
        return buckets;
    }

    public void setBuckets(Permissions buckets) {
        this.buckets = buckets;
    }

    @ApiModelProperty(
            value = "The access that the current user has to the top level /tenants resource of this NiFi Registry",
            readOnly = true)
    public Permissions getTenants() {
        return tenants;
    }

    public void setTenants(Permissions tenants) {
        this.tenants = tenants;
    }

    @ApiModelProperty(
            value = "The access that the current user has to the top level /policies resource of this NiFi Registry",
            readOnly = true)
    public Permissions getPolicies() {
        return policies;
    }

    public void setPolicies(Permissions policies) {
        this.policies = policies;
    }

    @ApiModelProperty(
            value = "The access that the current user has to the top level /proxy resource of this NiFi Registry",
            readOnly = true)
    public Permissions getProxy() {
        return proxy;
    }

    public void setProxy(Permissions proxy) {
        this.proxy = proxy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ResourcePermissions that = (ResourcePermissions) o;

        if (buckets != null ? !buckets.equals(that.buckets) : that.buckets != null)
            return false;
        if (tenants != null ? !tenants.equals(that.tenants) : that.tenants != null)
            return false;
        if (policies != null ? !policies.equals(that.policies) : that.policies != null)
            return false;
        return proxy != null ? proxy.equals(that.proxy) : that.proxy == null;
    }

    @Override
    public int hashCode() {
        int result = buckets != null ? buckets.hashCode() : 0;
        result = 31 * result + (tenants != null ? tenants.hashCode() : 0);
        result = 31 * result + (policies != null ? policies.hashCode() : 0);
        result = 31 * result + (proxy != null ? proxy.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ResourcePermissions{" +
                "buckets=" + buckets +
                ", tenants=" + tenants +
                ", policies=" + policies +
                ", proxy=" + proxy +
                '}';
    }
}
