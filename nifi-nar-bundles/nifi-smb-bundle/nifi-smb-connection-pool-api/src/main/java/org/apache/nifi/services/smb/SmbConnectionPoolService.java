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
package org.apache.nifi.services.smb;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import org.apache.nifi.controller.ControllerService;

public interface SmbConnectionPoolService extends ControllerService {

    /**
     * Returns the name of the share to connect.
     *
     * @return the share
     */
    String getShareName();

    /**
     * Returns the hostname to connect to.
     *
     * @return the hostname
     */
    String getHostname();

    /**
     * Returns the port using to connect.
     *
     * @return the port.
     */
    Integer getPort();

    /**
     * Returns the SmbClient to use
     *
     * @return the smbClient
     */
    SMBClient getSmbClient();

    /**
     * Returns the authentication context.
     *
     * @return the authentication context.
     */
    AuthenticationContext getAuthenticationContext();

}
