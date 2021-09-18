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
package org.apache.nifi.kerberos;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.security.krb.KerberosUser;

/**
 * Creates a KerberosUser which can be used for performing Kerberos authentication. Implementations should not cache a
 * KerberosUser and return it to multiple callers, as any given caller could call logout and thus impact other callers
 * with the same instance.
 *
 * It is the responsibility of the consumer to manage the lifecycle of the user by calling login and logout
 * appropriately. Generally the KerberosAction class should be used to execute an action as the given user with
 * proper handling of login/re-login.
 */
public interface KerberosUserService extends ControllerService {

    /**
     * Creates and returns a new instance of KerberosUser based on the configuration of the implementing service.
     *
     * @return a new KerberosUser instance
     */
    KerberosUser createKerberosUser();

}
