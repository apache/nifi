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
package org.apache.nifi.web.security.saml;

import org.apache.nifi.util.NiFiProperties;

public interface SAMLConfigurationFactory {

    /**
     * Creates a SAMLConfiguration instance from the given NiFiProperties.
     *
     * @param properties the NiFiProperties instance
     * @return the configuration instance
     * @throws Exception if the configuration can't be created
     */
    SAMLConfiguration create(final NiFiProperties properties) throws Exception;

}
