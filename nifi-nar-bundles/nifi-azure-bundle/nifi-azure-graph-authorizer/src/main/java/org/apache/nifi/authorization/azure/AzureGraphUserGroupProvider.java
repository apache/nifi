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
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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

    private int pageSize;
    private String claimForUserName;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    public static final String REFRESH_DELAY_PROPERTY = "REFRESH_DELAY";
    private static final long MINIMUM_SYNC_INTERVAL_MILLISECONDS = 10_000;
    public static final String AUTHORITY_ENDPOINT_PROPERTY = "AUTHORITY_ENDPOINT";
    public static final String TENANT_ID_PROPERTY = "TENANT_ID";
    public static final String APP_REG_CLIENT_ID_PROPERTY = "APP_REG_CLIENT_ID";
    public static final String APP_REG_CLIENT_SECRET_PROPERTY = "APP_REG_CLIENT_SECRET";
    // comma separate list of group names to search from AAD
    public static final String GROUP_FILTER_LIST_PROPERTY = "GROUP_FILTER_LIST_INCLUSION";
    // group filter with startswith
    public static final String GROUP_FILTER_PREFIX_PROPERTY = "GROUP_FILTER_PREFIX";
    // client side group filter 'endswith' operator, due to support limiation of
    // azure graph rest-api
    public static final String GROUP_FILTER_SUFFIX_PROPERTY = "GROUP_FILTER_SUFFIX";
    // client side group filter 'contains' operator, due to support limiation of
    // azure graph rest-api
    public static final String GROUP_FILTER_SUBSTRING_PROPERTY = "GROUP_FILTER_SUBSTRING";
    public static final String PAGE_SIZE_PROPERTY = "PAGE_SIZE";
    // default: upn (or userPrincipalName). possilbe choices ['upn', 'email']
    // this should be matched with oidc configuration in nifi.properties
    public static final String CLAIM_FOR_USERNAME = "CLAIM_FOR_USERNAME";
    public static final String DEFAULT_AAD_AUTHORITY_ENDPOINT = "https://login.microsoftonline.com";

    private ClientCredentialAuthProvider authProvider;
    private IGraphServiceClient graphClient;
    private AtomicReference<ImmutableAzureGraphUserGroup> azureGraphUserGroupRef = new AtomicReference<ImmutableAzureGraphUserGroup>();

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
        if (logger.isDebugEnabled()) {
            logger.debug("calling AzureGraphUserGroupProvder.initialize");
        }
    }

    private String getProperty(AuthorizerConfigurationContext authContext, String propertyName, String defaultValue) {
        final PropertyValue property = authContext.getProperty(propertyName);
        final String value;

        if (property != null && property.isSet()) {
            value = property.getValue();
        } else {
            value = defaultValue;
        }
        return value;
    }

    private long getDelayProperty(AuthorizerConfigurationContext authContext, String propertyName,
            String defaultValue) {
        final String propertyValue = getProperty(authContext, propertyName, defaultValue);
        final long syncInterval;
        try {
            syncInterval = Math.round(FormatUtils.getPreciseTimeDuration(propertyValue, TimeUnit.MILLISECONDS));
        } catch (final IllegalArgumentException ignored) {
            throw new AuthorizerCreationException(
                    String.format("The %s '%s' is not a valid time interval.", propertyName, propertyValue));
        }

        if (syncInterval < MINIMUM_SYNC_INTERVAL_MILLISECONDS) {
            throw new AuthorizerCreationException(String.format("The %s '%s' is below the minimum value of '%d ms'",
                    propertyName, propertyValue, MINIMUM_SYNC_INTERVAL_MILLISECONDS));
        }
        return syncInterval;
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        if (logger.isDebugEnabled()) {
            logger.debug("calling AzureGraphUserGroupProvder.onConfigured");
        }
        long fixedDelay = getDelayProperty(configurationContext, REFRESH_DELAY_PROPERTY, "5 mins");
        final String authorityEndpoint = getProperty(configurationContext, AUTHORITY_ENDPOINT_PROPERTY,
                DEFAULT_AAD_AUTHORITY_ENDPOINT);
        final String tenantId = getProperty(configurationContext, TENANT_ID_PROPERTY, null);
        final String clientId = getProperty(configurationContext, APP_REG_CLIENT_ID_PROPERTY, null);
        final String clientSecret = getProperty(configurationContext, APP_REG_CLIENT_SECRET_PROPERTY, null);
        this.pageSize = Integer.parseInt(getProperty(configurationContext, PAGE_SIZE_PROPERTY, "50"));
        this.claimForUserName = getProperty(configurationContext, CLAIM_FOR_USERNAME, "upn");

        if (StringUtils.isBlank(tenantId)) {
            throw new AuthorizerCreationException(
                    String.format("%s is a required field for AzureGraphUserGroupProvder", TENANT_ID_PROPERTY));
        }
        if (StringUtils.isBlank(clientId)) {
            throw new AuthorizerCreationException(
                    String.format("%s is a required field for AzureGraphUserGroupProvder", APP_REG_CLIENT_ID_PROPERTY));
        }
        if (StringUtils.isBlank(clientSecret)) {
            throw new AuthorizerCreationException(String.format("%s is a required field for AzureGraphUserGroupProvder",
                    APP_REG_CLIENT_SECRET_PROPERTY));
        }
        if (this.pageSize > 999) {
            throw new AuthorizerCreationException("Max page size for graph rest api call is 999.");
        }

        try {
            authProvider = new ClientCredentialAuthProvider.Builder()
                .authorityEndpoint(authorityEndpoint)
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();
            graphClient = GraphServiceClient.builder().authenticationProvider(authProvider).buildClient();
        } catch (ClientException ep) {
            throw new AuthorizerCreationException("Failed to create a GraphServiceClient", ep);
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
        if (StringUtils.isBlank(prefix) && StringUtils.isBlank(suffix) && StringUtils.isBlank(substring)
                && StringUtils.isBlank(groupFilterList)) {
            throw new AuthorizerCreationException("At least one GROUP_FILTER should be specified");
        }

        try {
            refreshUserGroup(groupFilterList, prefix, suffix, substring, pageSize);
        } catch (IOException | ClientException ep) {
            throw new AuthorizerCreationException("Failed to load user/group from Azure AD", ep);
        }
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                logger.info("scheduling refreshUserGroupData()");
                refreshUserGroup(groupFilterList, prefix, suffix, substring, pageSize);
            } catch (final Throwable t) {
                logger.error("Exception while refreshUserGroupData", t);
            }
        }, fixedDelay, fixedDelay, TimeUnit.MILLISECONDS);
    }

    private void refreshUserGroup(String groupFilterList, String prefix, String suffix, String substring, int pageSize) throws IOException, ClientException {
        final StopWatch stopWatch = new StopWatch(true);
        final Set<String> groupDisplayNames = getGroupsWith(groupFilterList, prefix, suffix, substring, pageSize);
        refreshUserGroupData(groupDisplayNames);
        stopWatch.stop();
        if (logger.isDebugEnabled()) {
            logger.debug("Refreshed {} user groups in {}", groupDisplayNames.size(), stopWatch.getDuration());
        }
    }

    private Set<String> getGroupsWith(String groupFilterList, String prefix, String suffix, String substring, int pageSize) {
        final StopWatch stopWatch = new StopWatch(true);
        final Set<String> groupDisplayNames = new HashSet<>();

        if (!StringUtils.isBlank(prefix) || !StringUtils.isBlank(suffix) || !StringUtils.isBlank(substring)) {
            groupDisplayNames.addAll(queryGroupsWith(prefix, suffix, substring, pageSize));
        }

        if (!StringUtils.isBlank(groupFilterList)) {
            groupDisplayNames.addAll(
                Arrays.stream(groupFilterList.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList())
            );
        }

        stopWatch.stop();

        if (logger.isDebugEnabled()) {
            logger.debug("Fetched {} groups in {}", groupDisplayNames.size(), stopWatch.getDuration());
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
        boolean filterEvaluation = false;
        IGroupCollectionRequest gRequest;
        IGroupCollectionPage filterResults;
        if (prefix != null && !prefix.isEmpty()) {
            // build a $filter query option and create a graph request if prefix is given
            final List<Option> requestOptions = new LinkedList<Option>();
            requestOptions.add(new QueryOption("$filter", String.format("startswith(displayName, '%s')", prefix)));
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
                filterEvaluation = true;
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
    private UserGroupQueryResult getUsersFrom(String groupName) throws IOException, ClientException {
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
                    final String idUser = jsonUser.get("id").getAsString();
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
        } else if (logger.isDebugEnabled()) {
            logger.debug("Group collection page for {} was null or empty", groupName);
        }
        return null;
    }

    /**
     * refresh the user & group data for UserGroupProvider plugin service
     * @param groupDisplayNames a list of group display names
     */
    private void refreshUserGroupData(Set<String> groupDisplayNames) throws IOException, ClientException {
        Objects.requireNonNull(groupDisplayNames);

        final long startTime = System.currentTimeMillis();
        final Set<User> _users = new HashSet<>();
        final Set<Group> _groups = new HashSet<>();

        for (String grpFilter : groupDisplayNames) {
            if (logger.isDebugEnabled()) logger.debug("Getting users for group filter: {}", grpFilter);
            UserGroupQueryResult queryResult = getUsersFrom(grpFilter);
            if (queryResult != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Found {} users for group: {}", queryResult.getUsers().size(), queryResult.getGroup().getName());
                }
                _groups.add(queryResult.getGroup());
                _users.addAll(queryResult.getUsers());
            } else {
                if (logger.isDebugEnabled()) logger.debug("Query result was null");
            }
        }
        final ImmutableAzureGraphUserGroup azureGraphUserGroup =
            ImmutableAzureGraphUserGroup.newInstance(_users, _groups);
        azureGraphUserGroupRef.set(azureGraphUserGroup);
        final long endTime = System.currentTimeMillis();
        if (logger.isDebugEnabled()) logger.debug("Refreshed users and groups, took {} miliseconds", (endTime - startTime));
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
        try {
            scheduler.shutdownNow();
        } catch (final Exception e) {
            logger.warn("Error shutting down refresh scheduler: " + e.getMessage(), e);
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
