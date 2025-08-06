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
package org.apache.nifi.registry.extension;

/**
 * The coordinate of a version of a bundle.
 *
 * BundlePersistenceProviders will be expected to retrieve the content of a given BundleVersionCoordinate.
 */
public interface BundleVersionCoordinate {

    /**
     * @return the NiFi Registry bucket id where the bundle is located
     */
    String getBucketId();

    /**
     * @return the group id of the bundle
     */
    String getGroupId();

    /**
     * @return the artifact id of the bundle
     */
    String getArtifactId();

    /**
     * @return the version of the bundle
     */
    String getVersion();

    /**
     * @return the type of the bundle
     */
    BundleVersionType getType();

    /**
     * @return the string representation of the coordinate
     */
    @Override
    String toString();
}
