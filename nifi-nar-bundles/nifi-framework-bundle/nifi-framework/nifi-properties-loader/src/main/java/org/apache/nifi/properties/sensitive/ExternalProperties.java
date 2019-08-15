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
package org.apache.nifi.authorization;

/**
<<<<<<< HEAD:nifi-framework-api/src/main/java/org/apache/nifi/authorization/AuthorizerLookup.java
 *
 */
public interface AuthorizerLookup {
=======
 * ExternalProperties is an interface for reading external values by name.
 *
 */
public interface ExternalProperties {
>>>>>>> NIFI-6326 GCP + Key Store + Vault + AWS combined WIP.:nifi-nar-bundles/nifi-framework-bundle/nifi-framework/nifi-properties-loader/src/main/java/org/apache/nifi/properties/sensitive/ExternalProperties.java

    /**
     * Looks up the Authorizer with the specified identifier
     *
     * @param identifier        The identifier of the Authorizer
     * @return                  The Authorizer
     */
    Authorizer getAuthorizer(String identifier);
}
