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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.gson.JsonObject;
import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.models.extensions.DirectoryObject;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.options.Option;
import com.microsoft.graph.options.QueryOption;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import com.microsoft.graph.requests.extensions.IDirectoryObjectCollectionWithReferencesPage;
import com.microsoft.graph.requests.extensions.IDirectoryObjectCollectionWithReferencesRequest;
import com.microsoft.graph.requests.extensions.IDirectoryObjectCollectionWithReferencesRequestBuilder;
import com.microsoft.graph.requests.extensions.IGroupCollectionPage;
import com.microsoft.graph.requests.extensions.IGroupCollectionRequest;
import com.microsoft.graph.requests.extensions.IGroupCollectionRequestBuilder;

import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AzureGraphUserGroupProvider provides support for retrieving users and
 * groups from Azure Activy Driectory (AAD) using graph rest-api & SDK.
 */
public class AzureGraphUserGroupProvider implements UserGroupProvider {
    private final static Logger logger = LoggerFactory.getLogger(AzureGraphUserGroupProvider.class);

    private String claimForUserName;

    private ScheduledExecutorService scheduler;

    public static final String REFRESH_DELAY_PROPERTY = "Refresh Delay";
    private static final long MINIMUM_SYNC_INTERVAL_MILLISECONDS = 10_000;
    public static final String AUTHORITY_ENDPOINT_PROPERTY = "Authority Endpoint";
    public static final String TENANT_ID_PROPERTY = "Directory ID";
    public static final String APP_REG_CLIENT_ID_PROPERTY = "Application ID";
    public static final String APP_REG_CLIENT_SECRET_PROPERTY = "Client Secret";
    // comma separated list of group names to search from AAD
    public static final String GROUP_FILTER_LIST_PROPERTY = "Group Filter List Inclusion";
    // group filter with startswith
    public static final String GROUP_FILTER_PREFIX_PROPERTY = "Group Filter Prefix";
    // client side group filter 'endswith' operator, due to support limiation of azure graph rest-api
    public static final String GROUP_FILTER_SUFFIX_PROPERTY = "Group Filter Suffix";
    // client side group filter 'contains' operator, due to support limiation of azure graph rest-api
    public static final String GROUP_FILTER_SUBSTRING_PROPERTY = "Group Filter Substring";
    public static final String PAGE_SIZE_PROPERTY = "Page Size";
    // default: upn (or userPrincipalName). possible choices ['upn', 'email']
    // this should be matched with oidc configuration in nifi.properties
    public static final String CLAIM_FOR_USERNAME = "Claim for Username";
    public static final String DEFAULT_REFRESH_DELAY = "5 mins";
    public static final String DEFAULT_PAGE_SIZE = "50";
    public static final String DEFAULT_CLAIM_FOR_USERNAME = "upn";
    public static final int MAX_PAGE_SIZE = 999;
    public static final String AZURE_PUBLIC_CLOUD = "https://login.microsoftonline.com/";
    static final List<String> REST_CALL_KEYWORDS = Arrays.asList("$select", "$top", "$expand", "$search", "$filter", "$format", "$count", "$skip", "$orderby");


    private ClientCredentialAuthProvider authProvider;
    private IGraphServiceClient graphClient;
    private final AtomicReference<ImmutableAzureGraphUserGroup> azureGraphUserGroupRef = new AtomicReference<ImmutableAzureGraphUserGroup>();

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        return azureGraphUserGroupRef.get().getGroup(identifier);
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return azureGraphUserGroupRef.get().getGroups();
    }

    @Override
    public User getUser(String identifier) throws AuthorizationAccessException {
        return azureGraphUserGroupRef.get().getUser(identifier);
    }

    @Override
    public UserAndGroups getUserAndGroups(String principalName) throws AuthorizationAccessException {
        return azureGraphUserGroupRef.get().getUserAndGroups(principalName);
    }

    @Override
    public User getUserByIdentity(String principalName) throws AuthorizationAccessException {
        return azureGraphUserGroupRef.get().getUserByPrincipalName(principalName);
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return azureGraphUserGroupRef.get().getUsers();
    }

    @Override
    public void initialize(UserGroupProviderInitializationContext initializationContext)
            throws AuthorizerCreationException {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                final Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setName(String.format("%s (%s) - UserGroup Refresh", getClass().getSimpleName(), initializationContext.getIdentifier()));
                return thread;
            }
        });
    }

    private String getProperty(AuthorizerConfigurationContext authContext, String propertyName, String defaultValue) {
        final PropertyValue property = authContext.getProperty(propertyName);

        if (property != null && property.isSet()) {
            final String value = property.getValue();
            if (StringUtils.isNotBlank(value)) {
                return value;
            }
        }

        return defaultValue;
    }

    private long getDelayProperty(AuthorizerConfigurationContext authContext, String propertyName, String defaultValue) {
        final String propertyValue = getProperty(authContext, propertyName, defaultValue);
        final long syncInterval;
        try {
            syncInterval = Math.round(FormatUtils.getPreciseTimeDuration(propertyValue, TimeUnit.MILLISECONDS));
        } catch (final IllegalArgumentException ignored) {
            throw new AuthorizerCreationException(String.format("The %s '%s' is not a valid time interval.", propertyName, propertyValue));
        }

        if (syncInterval < MINIMUM_SYNC_INTERVAL_MILLISECONDS) {
            throw new AuthorizerCreationException(String.format("The %s '%s' is below the minimum value of '%d ms'", propertyName, propertyValue, MINIMUM_SYNC_INTERVAL_MILLISECONDS));
        }
        return syncInterval;
    }

    private boolean hasReservedKeyword(String prefix) {
        return REST_CALL_KEYWORDS.contains(prefix);
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        final long fixedDelay = getDelayProperty(configurationContext, REFRESH_DELAY_PROPERTY, DEFAULT_REFRESH_DELAY);
        final String authorityEndpoint = getProperty(configurationContext, AUTHORITY_ENDPOINT_PROPERTY, AZURE_PUBLIC_CLOUD);
        final String tenantId = getProperty(configurationContext, TENANT_ID_PROPERTY, null);
        final String clientId = getProperty(configurationContext, APP_REG_CLIENT_ID_PROPERTY, null);
        final String clientSecret = getProperty(configurationContext, APP_REG_CLIENT_SECRET_PROPERTY, null);
        int pageSize = Integer.parseInt(getProperty(configurationContext, PAGE_SIZE_PROPERTY, DEFAULT_PAGE_SIZE));
        this.claimForUserName = getProperty(configurationContext, CLAIM_FOR_USERNAME, DEFAULT_CLAIM_FOR_USERNAME);
        final String providerClassName = getClass().getSimpleName();
        if (StringUtils.isBlank(tenantId)) {
            throw new AuthorizerCreationException(String.format("%s is a required field for %s", TENANT_ID_PROPERTY, providerClassName));
        }
        if (StringUtils.isBlank(clientId)) {
            throw new AuthorizerCreationException(String.format("%s is a required field for %s", APP_REG_CLIENT_ID_PROPERTY, providerClassName));
        }
        if (StringUtils.isBlank(clientSecret)) {
            throw new AuthorizerCreationException(String.format("%s is a required field for %s", APP_REG_CLIENT_SECRET_PROPERTY, providerClassName));
        }
        if (pageSize > MAX_PAGE_SIZE) {
            throw new AuthorizerCreationException(String.format("Max page size for Microsoft Graph is %d.", MAX_PAGE_SIZE));
        }

        try {
            authProvider = new ClientCredentialAuthProvider.Builder()
                .authorityEndpoint(authorityEndpoint)
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();
            graphClient = GraphServiceClient.builder().authenticationProvider(authProvider).buildClient();
        } catch (final ClientException e) {
            throw new AuthorizerCreationException(String.format("Failed to create a GraphServiceClient due to %s", e.getMessage()), e);
        }

        // first, load list of group name if there is any prefix, suffix, substring
        // filter defined, paging thru groups.
        // then, add additonal group list if there is group list inclusion defined.
        final String prefix = getProperty(configurationContext, GROUP_FILTER_PREFIX_PROPERTY, null);
        final String suffix = getProperty(configurationContext, GROUP_FILTER_SUFFIX_PROPERTY, null);
        final String substring = getProperty(configurationContext, GROUP_FILTER_SUBSTRING_PROPERTY, null);
        final String groupFilterList = getProperty(configurationContext, GROUP_FILTER_LIST_PROPERTY, null);

        // if no group filter is specified, generate exception since we don't want to
        // load whole groups from AAD.
        if (StringUtils.isBlank(prefix) && StringUtils.isBlank(suffix) && StringUtils.isBlank(substring) && StringUtils.isBlank(groupFilterList)) {
            throw new AuthorizerCreationException(String.format("At least one group filter (%s, %s, %s) should be specified for %s",
                GROUP_FILTER_PREFIX_PROPERTY, GROUP_FILTER_SUFFIX_PROPERTY, GROUP_FILTER_LIST_PROPERTY, providerClassName));
        }
        // make sure prefix shouldn't have any reserved keywords
        if (hasReservedKeyword(prefix)) {
            throw new AuthorizerCreationException(String.format("Prefix shouldn't have any reserved keywords ([%s])", StringUtils.join(REST_CALL_KEYWORDS, ",")));
        }

        try {
            refreshUserGroup(groupFilterList, prefix, suffix, substring, pageSize);
        } catch (final IOException | ClientException e) {
            throw new AuthorizerCreationException(String.format("Failed to load UserGroup due to %s", e.getMessage()), e);
        }
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                refreshUserGroup(groupFilterList, prefix, suffix, substring, pageSize);
            } catch (final Throwable t) {
                logger.error("Error refreshing user groups due to {}", t.getMessage(), t);
            }
        }, fixedDelay, fixedDelay, TimeUnit.MILLISECONDS);
    }

    private void refreshUserGroup(String groupFilterList, String prefix, String suffix, String substring, int pageSize) throws IOException, ClientException {
        if (logger.isDebugEnabled()) {
            logger.debug("Refreshing user groups");
        }
        final StopWatch stopWatch = new StopWatch(true);
        final Set<String> groupDisplayNames = getGroupsWith(groupFilterList, prefix, suffix, substring, pageSize);
        refreshUserGroupData(groupDisplayNames, pageSize);
        stopWatch.stop();
        if (logger.isDebugEnabled()) {
            logger.debug("Refreshed {} user groups in {}", groupDisplayNames.size(), stopWatch.getDuration());
        }
    }

    private Set<String> getGroupsWith(String groupFilterList, String prefix, String suffix, String substring, int pageSize) {
        final Set<String> groupDisplayNames = new HashSet<>();

        if (!StringUtils.isBlank(prefix) || !StringUtils.isBlank(suffix) || !StringUtils.isBlank(substring)) {
            groupDisplayNames.addAll(queryGroupsWith(prefix, suffix, substring, pageSize));
        }

        if (!StringUtils.isBlank(groupFilterList)) {
            groupDisplayNames.addAll(
                Arrays.stream(groupFilterList.split(","))
                    .map(String::trim)
                    .filter(s-> !s.isEmpty())
                    .collect(Collectors.toList())
            );
        }
        return Collections.unmodifiableSet(groupDisplayNames);
    }

    /**
     * Get a set of group display names after filtering prefix, suffix, and substring
     * @param prefix prefix filter string matching against displayName of group directory objects
     * @param suffix suffix fitler string matching against displayName of group directory objects
     * @param substring string matching against displayName of group directory objects
     * @param pageSize page size to make graph rest calls in pagination
     * @return set of group display names
     */
    private Set<String> queryGroupsWith(String prefix, String suffix, String substring, int pageSize) {
        final Set<String> groups = new HashSet<>();
        IGroupCollectionRequest gRequest;
        IGroupCollectionPage filterResults;
        if (prefix != null && !prefix.isEmpty()) {
            // build a $filter query option and create a graph request if prefix is given
            final List<Option> requestOptions = Arrays.asList(new QueryOption("$filter", String.format("startswith(displayName, '%s')", prefix)));
            gRequest = graphClient.groups().buildRequest(requestOptions).select("displayName");
        } else {
            // default group graph request
            gRequest = graphClient.groups().buildRequest().select("displayName");
        }
        if (pageSize > 0) {
            gRequest = gRequest.top(pageSize);
        }
        filterResults = gRequest.get();

        List<com.microsoft.graph.models.extensions.Group> currentPage = filterResults.getCurrentPage();
        while (currentPage != null) {
            for (com.microsoft.graph.models.extensions.Group grp : currentPage) {
                boolean filterEvaluation = true;
                if (!StringUtils.isEmpty(suffix) && !grp.displayName.endsWith(suffix)) {
                    filterEvaluation = false;
                }
                if (!StringUtils.isEmpty(substring) && !grp.displayName.contains(substring)) {
                    filterEvaluation = false;
                }
                if (filterEvaluation) {
                    groups.add(grp.displayName);
                }
            }
            IGroupCollectionRequestBuilder gBuilder = filterResults.getNextPage();
            if (gBuilder != null) {
                filterResults = gBuilder.buildRequest().get();
                currentPage = filterResults.getCurrentPage();
            } else {
                currentPage = null;
            }
        }

        return Collections.unmodifiableSet(groups);
    }

    /**
     * Get member users of the given group name
     * @param groupName group name to search for member users
     * @return UserGroupQueryResult
     */
    private UserGroupQueryResult getUsersFrom(String groupName, int pageSize) throws IOException, ClientException {
        final Set<User> users = new HashSet<>();

        final List<Option> requestOptions = Arrays.asList(new QueryOption("$filter", String.format("displayName eq '%s'", groupName)));
        final IGroupCollectionPage results = graphClient.groups().buildRequest(requestOptions).get();
        final List<com.microsoft.graph.models.extensions.Group> currentPage = results.getCurrentPage();

        if (currentPage != null && currentPage.size() > 0) {
            final com.microsoft.graph.models.extensions.Group graphGroup = results.getCurrentPage().get(0);
            final Group.Builder groupBuilder =
                new Group.Builder()
                    .identifier(graphGroup.id)
                    .name(graphGroup.displayName);

            IDirectoryObjectCollectionWithReferencesRequest uRequest =
                graphClient.groups(graphGroup.id)
                    .members()
                    .buildRequest()
                    .select("id, displayName, mail, userPrincipalName");

            if (pageSize > 0) {
                uRequest = uRequest.top(pageSize);
            }
            IDirectoryObjectCollectionWithReferencesPage userpage =
                graphClient.groups(graphGroup.id)
                    .members()
                    .buildRequest()
                    .select("id, displayName, mail, userPrincipalName").get();

            while (userpage.getCurrentPage() != null) {
                for (DirectoryObject userDO : userpage.getCurrentPage()) {
                    JsonObject jsonUser = userDO.getRawObject();
                    final String idUser;
                    if (!jsonUser.get("id").isJsonNull()) {
                        idUser = jsonUser.get("id").getAsString();
                    } else {
                        idUser = "";
                    }
                    // upn is default fallback claim for userName
                    // upn claim maps to 'mail' property in Azure graph rest-api.
                    final String userName;
                    if (claimForUserName.equals("email")) {
                        // authentication token contains email field, while graph api returns mail property
                        if (!jsonUser.get("mail").isJsonNull()) {
                            userName = jsonUser.get("mail").getAsString();
                        } else {
                            userName = jsonUser.get("userPrincipalName").getAsString();
                        }
                    } else {
                        userName = jsonUser.get("userPrincipalName").getAsString();
                    }
                    final User user = new User.Builder().identifier(idUser).identity(userName).build();
                    users.add(user);
                    groupBuilder.addUser(idUser);
                }
                IDirectoryObjectCollectionWithReferencesRequestBuilder nextPageRequest = userpage.getNextPage();

                if (nextPageRequest != null) {
                    userpage = nextPageRequest.buildRequest().get();
                } else {
                    break;
                }
            }
            final Group group = groupBuilder.build();
            return new UserGroupQueryResult(group, users);
        }
        return null;
    }

    /**
     * refresh the user & group data for UserGroupProvider plugin service
     * @param groupDisplayNames a list of group display names
     */
    private void refreshUserGroupData(Set<String> groupDisplayNames, int pageSize) throws IOException, ClientException {
        Objects.requireNonNull(groupDisplayNames);

        final Set<User> users = new HashSet<>();
        final Set<Group> groups = new HashSet<>();

        for (String grpFilter : groupDisplayNames) {
            if (logger.isDebugEnabled()) logger.debug("Getting users for group filter: {}", grpFilter);
            UserGroupQueryResult queryResult = getUsersFrom(grpFilter, pageSize);
            if (queryResult != null) {
                groups.add(queryResult.getGroup());
                users.addAll(queryResult.getUsers());
            }
        }
        final ImmutableAzureGraphUserGroup azureGraphUserGroup = ImmutableAzureGraphUserGroup.newInstance(users, groups);
        azureGraphUserGroupRef.set(azureGraphUserGroup);
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

    private static class UserGroupQueryResult {
        private final Group group;
        private final Set<User> users;

        public UserGroupQueryResult(Group group, Set<User> users) {
            this.group = group;
            this.users = users;
        }

        public Group getGroup() {
            return this.group;
        }

        public Set<User> getUsers() {
            return this.users;
        }
    }

}
