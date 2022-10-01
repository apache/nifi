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
package org.apache.nifi.authorization.azure;

import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.models.extensions.AppRole;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.EMPTY_SET;

/**
 * The AzureAppRolesUserGroupProvider provides support for retrieving app roles
 * from Azure Active Directory (AAD) using graph rest-api & SDK.
 */
public class AzureAppRolesUserGroupProvider extends AbstractAzureUserGroupProvider implements UserGroupProvider  {
    private final static Logger logger = LoggerFactory.getLogger(AzureAppRolesUserGroupProvider.class);
    public static final String APP_REG_OBJECT_ID_PROPERTY = "Object ID";

    private ClientCredentialAuthProvider authProvider;
    private IGraphServiceClient graphClient;
    private ScheduledExecutorService scheduler;
    private final AtomicReference<ImmutableAzureGraphUserGroup> azureGraphUserGroupRef = new AtomicReference<ImmutableAzureGraphUserGroup>();

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return azureGraphUserGroupRef.get().getUsers();
    }

    @Override
    public User getUser(String identifier) throws AuthorizationAccessException {
        return null;
    }

    @Override
    public User getUserByIdentity(String identity) throws AuthorizationAccessException {
        return null;
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return azureGraphUserGroupRef.get().getGroups();
    }

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        return azureGraphUserGroupRef.get().getGroup(identifier);
    }

    @Override
    public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
        return new UserAndGroups() {
            @Override public User getUser() {
                return azureGraphUserGroupRef.get().getUser(identity);
            }
            @Override public Set<Group> getGroups() {
                return null;
            }
        };
    }

    @Override
    public void initialize(UserGroupProviderInitializationContext initializationContext)
            throws AuthorizerCreationException {
        this.scheduler = newScheduler(initializationContext);
    }


    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        final long fixedDelay = getDelayProperty(configurationContext, REFRESH_DELAY_PROPERTY, DEFAULT_REFRESH_DELAY);
        final String providerClassName = getClass().getSimpleName();

        final String objectId = getProperty(configurationContext, APP_REG_OBJECT_ID_PROPERTY, null);
        if (StringUtils.isBlank(objectId)) {
            throw new AuthorizerCreationException(String.format("%s is a required field for %s", APP_REG_OBJECT_ID_PROPERTY, providerClassName));
        }

        try {
            authProvider = getClientCredentialAuthProvider(configurationContext);
            graphClient = getGraphServiceClient(authProvider);
        } catch (final AuthorizerCreationException e) {
            throw new AuthorizerCreationException(String.format("%s for %s", e.getMessage(), providerClassName));
        } catch (final ClientException e) {
            throw new AuthorizerCreationException(String.format("Failed to create a GraphServiceClient due to %s", e.getMessage()), e);
        }

        Runnable refresh = () -> {
            try {
                refreshUserGroup(objectId);
            } catch (final Throwable t) {
                logger.error("Error refreshing user groups due to {}", t.getMessage(), t);
            }
        };
        refresh.run();
        scheduler.scheduleWithFixedDelay(refresh, 0, fixedDelay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (final InterruptedException e) {
            logger.warn("Error shutting down user group refresh scheduler due to {}", e.getMessage(), e);
        }
    }

    private void refreshUserGroup(String objectId) throws IOException, ClientException {
        if (logger.isDebugEnabled()) {
            logger.debug("Refreshing app roles");
        }
        final StopWatch stopWatch = new StopWatch(true);
        final List<AppRole> appRoles = getAppRoles(objectId);
        refreshUserGroupData(appRoles);
        stopWatch.stop();
        if (logger.isDebugEnabled()) {
            logger.debug("Refreshed {} app roles in {}", appRoles.size(), stopWatch.getDuration());
        }
    }

    private void refreshUserGroupData(List<AppRole> appRoles) {
        //final Set<User> users = appRoles.stream().map(role -> new User.Builder().identifier(role.id.toString()).identity(role.displayName).build()).collect(Collectors.toSet());
        final Set<User> users = EMPTY_SET;
        final Set<Group> groups = appRoles.stream().map(role -> new Group.Builder().identifier(role.id.toString()).name(role.displayName).build()).collect(Collectors.toSet());
        final ImmutableAzureGraphUserGroup azureGraphUserGroup = ImmutableAzureGraphUserGroup.newInstance(users, groups);
        azureGraphUserGroupRef.set(azureGraphUserGroup);
    }

    private List<AppRole> getAppRoles(String objectId) {
        return graphClient.applications(objectId).buildRequest().get().appRoles;
    }
}
