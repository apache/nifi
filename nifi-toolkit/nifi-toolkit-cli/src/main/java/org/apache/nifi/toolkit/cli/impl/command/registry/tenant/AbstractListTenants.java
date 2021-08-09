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
package org.apache.nifi.toolkit.cli.impl.command.registry.tenant;

import org.apache.commons.cli.ParseException;
import org.apache.nifi.registry.authorization.Tenant;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.TenantsClient;
import org.apache.nifi.toolkit.cli.api.Result;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;

import java.io.IOException;
import java.util.Properties;

/**
 * Abstract command to get the list of tenants of a specific type.
 *
 * @param <T> The type of tenants in the result.
 * @param <R> The type of the result object.
 */
public abstract class AbstractListTenants<T extends Tenant, R extends Result> extends AbstractNiFiRegistryCommand<R> {
    public AbstractListTenants(String name, Class<R> resultClass) {
        super(name, resultClass);
    }

    @Override
    public R doExecute(final NiFiRegistryClient client, final Properties properties)
        throws IOException, NiFiRegistryException, ParseException {

        final TenantsClient tenantsClient = client.getTenantsClient();

        return getTenants(properties, tenantsClient);
    }

    protected abstract R getTenants(Properties properties, TenantsClient tenantsClient) throws NiFiRegistryException, IOException;
}
