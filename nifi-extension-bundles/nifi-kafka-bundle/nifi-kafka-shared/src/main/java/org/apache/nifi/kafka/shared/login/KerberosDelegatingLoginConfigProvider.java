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
package org.apache.nifi.kafka.shared.login;

import org.apache.nifi.context.PropertyContext;

/**
 * Kerberos Delegating Login Module implementation of configuration provider
 */
public class KerberosDelegatingLoginConfigProvider implements LoginConfigProvider {
    private static final LoginConfigProvider USER_SERVICE_PROVIDER = new KerberosUserServiceLoginConfigProvider();

    /**
     * Get JAAS configuration using configured Kerberos credentials
     *
     * @param context Property Context
     * @return JAAS configuration with Kerberos Login Module
     */
    @Override
    public String getConfiguration(final PropertyContext context) {
        return USER_SERVICE_PROVIDER.getConfiguration(context);
    }
}
