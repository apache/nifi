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
package org.apache.nifi.toolkit.cli.impl.command.nifi;

import com.opencsv.CSVParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.ClientFactory;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Result;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.AbstractPropertyCommand;
import org.apache.nifi.toolkit.cli.impl.session.SessionVariable;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.TenantDTO;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.apache.nifi.web.api.entity.UserGroupsEntity;
import org.apache.nifi.web.api.entity.UsersEntity;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base class for all NiFi commands.
 */
public abstract class AbstractNiFiCommand<R extends Result> extends AbstractPropertyCommand<R> {

    public AbstractNiFiCommand(final String name, final Class<R> resultClass) {
        super(name, resultClass);
    }

    @Override
    protected SessionVariable getPropertiesSessionVariable() {
        return SessionVariable.NIFI_CLIENT_PROPS;
    }

    @Override
    public final R doExecute(final Properties properties) throws CommandException {
        final ClientFactory<NiFiClient> clientFactory = getContext().getNiFiClientFactory();
        try (final NiFiClient client = clientFactory.createClient(properties)) {
            return doExecute(client, properties);
        } catch (Exception e) {
            throw new CommandException("Error executing command '" + getName() + "' : " + e.getMessage(), e);
        }
    }

    /**
     * Sub-classes implement to perform the desired action using the provided client and properties.
     *
     * @param client a NiFi client
     * @param properties properties for the command
     * @return the Result of executing the command
     */
    public abstract R doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException;


    protected RevisionDTO getInitialRevisionDTO() {
        final String clientId = getContext().getSession().getNiFiClientID();

        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setVersion(new Long(0));
        revisionDTO.setClientId(clientId);
        return revisionDTO;
    }

    protected static Set<TenantEntity> generateTenantEntities(final String ids)
        throws IOException {
        final CSVParser csvParser = new CSVParser();
        return Arrays.stream(csvParser.parseLine(ids))
            .map(AbstractNiFiCommand::createTenantEntity)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    protected static Set<TenantEntity> generateTenantEntities(final String users, final UsersEntity existingUsers)
        throws IOException, CommandException {
        final CSVParser csvParser = new CSVParser();
        final String[] userArray = csvParser.parseLine(users);
        final Set<TenantEntity> tenantEntities = new LinkedHashSet<>();

        for (String user : userArray) {
            Optional<UserEntity> existingUser = existingUsers.getUsers().stream()
                .filter(entity -> user.equals(entity.getComponent().getIdentity())).findAny();

            if (!existingUser.isPresent()) {
                throw new CommandException("User with the identity '" + user + "' not found.");
            }

            tenantEntities.add(createTenantEntity(existingUser.get().getId(), user));
        }

        return tenantEntities;
    }

    protected static Set<TenantEntity> generateTenantEntities(final String groups, final UserGroupsEntity existingGroups)
        throws IOException, CommandException {
        final CSVParser csvParser = new CSVParser();
        final String[] groupArray = csvParser.parseLine(groups);
        final Set<TenantEntity> tenantEntities = new LinkedHashSet<>();

        for (String group : groupArray) {
            Optional<UserGroupEntity> existingGroup = existingGroups.getUserGroups().stream()
                .filter(entity -> group.equals(entity.getComponent().getIdentity())).findAny();

            if (!existingGroup.isPresent()) {
                throw new CommandException("User group with the identity '" + group + "' not found.");
            }

            tenantEntities.add(createTenantEntity(existingGroup.get().getId(), group));
        }

        return tenantEntities;
    }

    private static TenantEntity createTenantEntity(final String id) {
        return createTenantEntity(id, null);
    }

    private static TenantEntity createTenantEntity(final String id, final String identity) {
        TenantEntity tenantEntity = new TenantEntity();
        tenantEntity.setId(id);

        if (StringUtils.isNotBlank(identity)) {
            TenantDTO tenantDTO = new TenantDTO();
            tenantDTO.setIdentity(identity);
            tenantEntity.setComponent(tenantDTO);
        }

        return tenantEntity;
    }
}
