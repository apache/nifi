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
package org.apache.nifi.registry.security.authorization.resource;

public enum ResourceType {
    Bucket("/buckets"),
    Policy("/policies"),
    Proxy("/proxy"),
    Tenant("/tenants"),
    Actuator("/actuator"),
    Swagger("/swagger");

    final String value;

    private ResourceType(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static ResourceType valueOfValue(final String rawValue) {
        ResourceType type = null;

        for (final ResourceType rt : values()) {
            if (rt.getValue().equals(rawValue)) {
                type = rt;
                break;
            }
        }

        if (type == null) {
            throw new IllegalArgumentException("Unknown resource type value " + rawValue);
        }

        return type;
    }

    /**
     * Map an arbitrary resource path to its base resource type. The base resource type is
     * what the resource path starts with.
     *
     * The resourcePath arg is expected to be a string of the format:
     *
     * {ResourceTypeValue}/arbitrary/sub-resource/path
     *
     * For example:
     *   /buckets -> ResourceType.Bucket
     *   /buckets/bucketId -> ResourceType.Bucket
     *   /policies/read/buckets -> ResourceType.Policy
     *
     * @param resourcePath the path component of a URI (not including the context path)
     * @return the base resource type
     */
    public static ResourceType mapFullResourcePathToResourceType(final String resourcePath) {
        if (resourcePath == null) {
            throw new IllegalArgumentException("Resource path must not be null");
        }

        ResourceType type = null;

        for (final ResourceType rt : values()) {
            final String rtValue = rt.getValue();
            if(resourcePath.equals(rtValue) || resourcePath.startsWith(rtValue + "/"))  {
                type = rt;
                break;
            }
        }

        return type;
    }
}