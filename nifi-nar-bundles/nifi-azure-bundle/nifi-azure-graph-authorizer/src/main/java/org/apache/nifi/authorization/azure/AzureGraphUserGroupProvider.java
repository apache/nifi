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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AzureGraphUserGroupProvider provides support for retrieving users and
 * groups from Azure Activy Driectory (AAD) using graph rest-api & SDK. This
 * providers will first search and filter the groups based on a prefix, a
 * suffix, and a substring filter, if any specified. Comparsion is evaluated
 * against 'displayName' property of Group directory object. Then, it will add
 * optional list of groups, separated by comma, into the list of group. If no
 * prefix/suffix/substring group filter is specified for group filter, it will
 * just use the optional list of groups for loading group directory info
 * (displayName and id). Finally, it will finds all the member users of these
 * groups. Only those users with group membership will be loaded into this
 * provider.
 * </p>
 * Pre-requsites on Azure Activy Driectory (AAD) side: As described in
 * <a href="https://docs.microsoft.com/en-us/graph/auth-v2-service">Azure graph doc</a>,
 *  <ul>
 *      <li>Register your NiFi Application to AAD</li>
 *      <li>Configure application type permissions for Microsoft Graph on your app (Group.Read.All,User.Read.All)</li>
 *      <li>Grant admin consent for the configured permissions</li>
 *      <li>Create a client secret for your application</li>
 *  </ul>
 * </ul>
 * </p>
 * Reference the following document and sample to configure pre-requsites for detail instructions.
 *  <ul>
 *      <li> <a href="https://docs.microsoft.com/en-us/azure/active-directory/develop/scenario-daemon-app-registration">document</a></li>
 *      <li> <a href="https://github.com/Azure-Samples/ms-identity-java-daemon">sample</a></li>
 *  </ul>
 * </p>
 * Enable 'upn' optional claim for ID token using 
 * <a href="https://docs.microsoft.com/en-us/azure/active-directory/develop/active-directory-optional-claims#configuring-optional-claims">
 * Azure AAD document</a> and disable 'Replace has marks' option.
 * </p>
 * And, set nifi.security.user.oidc.claim.identifying.user=upn in nifi.properties file.
 * </p> 
 */
public class AzureGraphUserGroupProvider implements UserGroupProvider {
    private final static Logger logger = LoggerFactory.getLogger(AzureGraphUserGroupProvider.class);
    private List<String> groupFilterList;
    private int pageSize;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    public static final String REFRESH_DELAY_PROPERTY = "REFRESH_DELAY";
    private static final long MINIMUM_SYNC_INTERVAL_MILLISECONDS = 10_000;
    // global authority endpoint: https://login.microsoftonline.com,
    // gov cloud authority endpoint: https://login.microsoftonline.us
    public static final String AUTHORITY_ENDPOINT_PROPERTY = "AUTHORITY_ENDPOINT";
    public static final String TENANT_ID_PROPERTY = "TENANT_ID";
    public static final String APP_REG_CLIENT_ID_PROPERTY = "APP_REG_CLIENT_ID";
    public static final String APP_REG_CLIENT_SECRET_PROPERTY = "APP_REG_CLIENT_SECRET";
    // comma separate list of group names to search from AAD
    public static final String GROUP_FILTER_LIST_PROPERTY = "GROUP_FILTER_LIST_INCLUSION";
    // group filter with startswith
    public static final String GROUP_FILTER_PREFIX_PROPERTY = "GROUP_FILTER_PREFIX";
    // client side group filter 'endswith' operator, due to support limiation of
    // azure graph rest
    public static final String GROUP_FILTER_SUFFIX_PROPERTY = "GROUP_FILTER_SUFFIX";
    // client side group filter 'contains' operator, due to support limiation of
    // azure graph rest
    public static final String GROUP_FILTER_SUBSTRING_PROPERTY = "GROUP_FILTER_SUBSTRING";
    public static final String PAGE_SIZE_PROPERTY = "PAGE_SIZE";
    private final Map<String, User> usersById = new HashMap<String, User>(); // id == identifier
    private final Map<String, User> usersByName = new HashMap<String, User>(); // name == identity
    private final Map<String, Group> groupsById = new HashMap<String, Group>();

    private ClientCrednetialAuthProvider authProvider;
    private IGraphServiceClient graphClient;

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        Group group;

        synchronized (groupsById) {
            group = groupsById.get(identifier);
        }
        return group;
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        synchronized (groupsById) {
            logger.debug("getGroups has group set of size: " + groupsById.size());
            return groupsById.values().stream().collect(Collectors.toSet());
        }
    }

    @Override
    public User getUser(String identifier) throws AuthorizationAccessException {
        User user;

        synchronized (usersById) {
            user = usersById.get(identifier);
        }

        if (user == null) {
            logger.debug("getUser (by id) user not found: " + identifier);
        } else {
            logger.debug("getUser (by id) found user: " + user + " for id: " + identifier);
        }
        return user;
    }

    @Override
    public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
        final User user = getUserByIdentity(identity);
        return new UserAndGroups() {
            @Override
            public User getUser() {
                return user;
            }

            @Override
            public Set<Group> getGroups() {
                if (user == null) {
                    return null;
                } else {
                    return groupsById.values().stream().filter(group -> group.getUsers().contains(user.getIdentifier()))
                            .collect(Collectors.toSet());
                }
            }
        };
    }

    @Override
    public User getUserByIdentity(String identity) throws AuthorizationAccessException {
        User user;

        synchronized (usersByName) {
            user = usersByName.get(identity);
        }

        if (user == null) {
            logger.debug("getUser (by name) user not found: " + identity);
        } else {
            logger.debug("getUser (by name) found user: " + user.getIdentity() + " for name: " + identity);
        }
        return user;
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        synchronized (usersById) {
            logger.debug("getUsers has user set of size: " + usersById.size());
            return usersById.values().stream().collect(Collectors.toSet());
        }
    }

    @Override
    public void initialize(UserGroupProviderInitializationContext initializationContext)
            throws AuthorizerCreationException {
        logger.debug("calling AzureGraphUserGroupProvder.initialize");

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
        logger.debug("calling AzureGraphUserGroupProvder.onConfigured");
        long fixedDelay = getDelayProperty(configurationContext, REFRESH_DELAY_PROPERTY, "5 mins");
        final String authority_endpoint = getProperty(configurationContext, AUTHORITY_ENDPOINT_PROPERTY,
                "https://login.microsoftonline.com");
        final String tenant_id = getProperty(configurationContext, TENANT_ID_PROPERTY, null);
        final String client_id = getProperty(configurationContext, APP_REG_CLIENT_ID_PROPERTY, null);
        final String client_secret = getProperty(configurationContext, APP_REG_CLIENT_SECRET_PROPERTY, null);
        this.pageSize = Integer.parseInt(getProperty(configurationContext, PAGE_SIZE_PROPERTY, "50"));

        if (StringUtils.isEmpty(tenant_id)) {
            throw new AuthorizerCreationException(
                    String.format("%s is a required field for AzureGraphUserGroupProvder", TENANT_ID_PROPERTY));
        }
        if (StringUtils.isEmpty(client_id)) {
            throw new AuthorizerCreationException(
                    String.format("%s is a required field for AzureGraphUserGroupProvder", APP_REG_CLIENT_ID_PROPERTY));
        }
        if (StringUtils.isEmpty(client_secret)) {
            throw new AuthorizerCreationException(String.format("%s is a required field for AzureGraphUserGroupProvder",
                    APP_REG_CLIENT_SECRET_PROPERTY));
        }
        if(this.pageSize > 999) {
            throw new AuthorizerCreationException("Max page size for graph rest api call is 999.");
        }

        try {
            authProvider = new ClientCrednetialAuthProvider.Builder().authorityEndpoint(authority_endpoint)
                    .tenantId(tenant_id).clientId(client_id).clientSecret(client_secret).build();

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
        final String group_filter_lst = getProperty(configurationContext, GROUP_FILTER_LIST_PROPERTY, null);

        // if no group filter is specified, generate exception since we don't want to
        // load whole groups from AAD.
        if (StringUtils.isEmpty(prefix) && StringUtils.isEmpty(suffix) && StringUtils.isEmpty(substring)
                && StringUtils.isEmpty(group_filter_lst)) {
            throw new AuthorizerCreationException("At least one GROUP_FILTER should be specified");
        }
        if (!StringUtils.isEmpty(prefix) || !StringUtils.isEmpty(suffix) || !StringUtils.isEmpty(substring)) {
            this.groupFilterList = getGroupListWith(prefix, suffix, substring, pageSize);
        }

        if (!StringUtils.isEmpty(group_filter_lst)) {
            final List<String> gList = Arrays.stream(group_filter_lst.split(",")).map(String::trim)
                    .collect(Collectors.toList());
            if (groupFilterList != null) {
                Set<String> gNameSet = this.groupFilterList.stream().collect(Collectors.toSet());
                this.groupFilterList
                        .addAll(gList.stream().filter(gname -> !gNameSet.contains(gname)).collect(Collectors.toSet()));

            } else {
                groupFilterList = gList;
            }
        }
        try {
            refreshUserGroupData(groupFilterList);
        } catch (IOException | ClientException ep) {
            throw new AuthorizerCreationException("Failed to load user/group from Azure AD", ep);
        }
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                logger.info("scheduling refreshUserGroupData()");
                refreshUserGroupData(groupFilterList);
            } catch (final Throwable t) {
                logger.error("", t);
            }
        }, fixedDelay, fixedDelay, TimeUnit.MILLISECONDS);
    }

    /**
     * Get list of group display names after filtering prefix, suffix, and substring
     * @param prefix prefix filter string matching against displayName of group directory objects
     * @param suffix suffix fitler string matching against displayName of group directory objects
     * @param substring string matching against displayName of group directory objects
     * @param pageSize page size to make graph rest calls in pagination
     * @return list of group display names
     */
    private List<String> getGroupListWith(String prefix, String suffix, String substring, int pageSize) {
        List<String> gList = new ArrayList<String>();
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
                    gList.add(grp.displayName);
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

        return gList;
    }

    /**
     * Get member users of the given group name
     * @param groupName group name to search for member users
     * @return UserGroupQueryResult
     */
    private UserGroupQueryResult getUsersFrom(String groupName) throws IOException, ClientException {
        final Map<String, User> idUserMap = new HashMap<>(); // id -> User
        final Map<String, User> nameUserMap = new HashMap<>(); // name -> User

        final List<Option> requestOptions = new LinkedList<Option>();
        requestOptions.add(new QueryOption("$filter", String.format("displayName eq '%s'", groupName)));

        IGroupCollectionPage results = graphClient.groups().buildRequest(requestOptions).get();

        if (results.getCurrentPage() != null) {
            final com.microsoft.graph.models.extensions.Group graphGroup = results.getCurrentPage().get(0);
            final Group.Builder groupBuilder = new Group.Builder().identifier(graphGroup.id)
                    .name(graphGroup.displayName);
            IDirectoryObjectCollectionWithReferencesRequest uRequest = graphClient.groups(graphGroup.id).members()
                    .buildRequest().select("id, displayName, mail, userPrincipalName");

            if (pageSize > 0) {
                uRequest = uRequest.top(pageSize);
            }

            IDirectoryObjectCollectionWithReferencesPage userpage = graphClient.groups(graphGroup.id).members()
                    .buildRequest().select("id, displayName, mail, userPrincipalName").get();

            while (userpage.getCurrentPage() != null) {
                for (DirectoryObject userDO : userpage.getCurrentPage()) {
                    JsonObject jsonUser = userDO.getRawObject();
                    final String idUser = jsonUser.get("id").getAsString();
                    final String userName;
                    if (!jsonUser.get("userPrincipalName").isJsonNull()) {
                        userName = jsonUser.get("userPrincipalName").getAsString();
                    } else if (!jsonUser.get("mail").isJsonNull()) {
                        userName = jsonUser.get("mail").getAsString();
                    } else {
                        userName = jsonUser.get("displayName").getAsString();
                    }
                    final User user = new User.Builder().identifier(idUser).identity(userName).build();
                    idUserMap.put(idUser, user);
                    nameUserMap.put(userName, user);
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
            return new UserGroupQueryResult(group, idUserMap, nameUserMap);
        } else {
            return null;

        }
    }

    /**
     * refresh the user & group data for UserGroupProvider plugin service
     * @param groupDisplayNames a list of group display names
     */
    private void refreshUserGroupData(List<String> groupDisplayNames) throws IOException, ClientException {
        final long startTime = System.currentTimeMillis();
        // load data into temporary data structure before swapping out
        final Map<String, User> _usersById = new HashMap<String, User>(); // id == identifier
        final Map<String, User> _usersByName = new HashMap<String, User>(); // name == identity
        final Map<String, Group> _groupsById = new HashMap<String, Group>();

        for (String grpFilter : groupDisplayNames) {

            UserGroupQueryResult queryResult = getUsersFrom(grpFilter);
            if (queryResult != null) {
                _usersById.putAll(queryResult.idToUserMap);
                _usersByName.putAll(queryResult.nameToUserMap);
                _groupsById.put(queryResult.group.getIdentifier(), queryResult.group);
            }
        }

        synchronized (usersById) {
            usersById.clear();
            usersById.putAll(_usersById);
        }

        synchronized (usersByName) {
            usersByName.clear();
            usersByName.putAll(_usersByName);
        }

        synchronized (groupsById) {
            groupsById.clear();
            groupsById.putAll(_groupsById);
        }
        final long endTime = System.currentTimeMillis();
        logger.info("Refreshed users and groups, took {} miliseconds", (endTime - startTime));

    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
        try {
            scheduler.shutdownNow();
        } catch (final Exception e) {
            logger.warn("Error shutting down refresh scheduler: " + e.getMessage(), e);
        }
    }

    class UserGroupQueryResult {
        Group group;
        Map<String, User> idToUserMap;
        Map<String, User> nameToUserMap;

        public UserGroupQueryResult(Group group, Map<String, User> idToUserMap, Map<String, User> nameToUserMap) {
            this.group = group;
            this.idToUserMap = idToUserMap;
            this.nameToUserMap = nameToUserMap;
        }
    }

}