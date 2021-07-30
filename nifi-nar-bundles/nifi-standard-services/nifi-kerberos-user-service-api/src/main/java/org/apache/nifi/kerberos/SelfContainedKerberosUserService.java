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

/**
 * A KerberosUserService where calling getConfigurationEntry() on the returned KerberosUser will produce a self-contained
 * JAAS AppConfigurationEntry that can be used to perform a login.
 *
 * As an example, keytab-based login with Krb5LoginModule is self-contained because the AppConfigurationEntry has the principal
 * and keytab location which is all the information required to perform the login, whereas password-based login with Krb5LoginModule
 * is not self-contained because a specific CallbackHandler must be configured to provide the username & password.
 */
public interface SelfContainedKerberosUserService extends KerberosUserService {

}
