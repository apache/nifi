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
package org.apache.nifi.documentation;

/**
 * Describes a Controller Service API that is provided by some implementation
 */
public interface ProvidedServiceAPI {
    /**
     * @return the fully qualified class name of the interface implemented by the Controller Service
     */
    String getClassName();

    /**
     * @return the Group ID of the bundle that provides the interface
     */
    String getGroupId();

    /**
     * @return the Artifact ID of the bundle that provides the interface
     */
    String getArtifactId();

    /**
     * @return the Version of the bundle that provides the interface
     */
    String getVersion();
}
