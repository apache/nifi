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
package org.apache.nifi.ldap.tenants;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authentication.exception.ProviderDestructionException;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.authorization.annotation.AuthorizerContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.ldap.LdapAuthenticationStrategy;
import org.apache.nifi.ldap.LdapsSocketFactory;
import org.apache.nifi.ldap.ReferralStrategy;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.SslContextFactory.ClientAuth;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ldap.control.PagedResultsDirContextProcessor;
import org.springframework.ldap.core.ContextSource;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.DirContextOperations;
import org.springframework.ldap.core.DirContextProcessor;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.LdapTemplate.NullDirContextProcessor;
import org.springframework.ldap.core.support.AbstractContextMapper;
import org.springframework.ldap.core.support.AbstractTlsDirContextAuthenticationStrategy;
import org.springframework.ldap.core.support.DefaultTlsDirContextAuthenticationStrategy;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.ldap.core.support.SimpleDirContextAuthenticationStrategy;
import org.springframework.ldap.core.support.SingleContextSource;
import org.springframework.ldap.filter.AndFilter;
import org.springframework.ldap.filter.EqualsFilter;
import org.springframework.ldap.filter.HardcodedFilter;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchControls;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract LDAP based implementation of a login identity provider.
 */
public class LdapUserGroupProvider implements UserGroupProvider {

    private static final Logger logger = LoggerFactory.getLogger(LdapUserGroupProvider.class);

    public static final String PROP_CONNECT_TIMEOUT = "Connect Timeout";
    public static final String PROP_READ_TIMEOUT = "Read Timeout";
    public static final String PROP_AUTHENTICATION_STRATEGY = "Authentication Strategy";
    public static final String PROP_MANAGER_DN = "Manager DN";
    public static final String PROP_MANAGER_PASSWORD = "Manager Password";
    public static final String PROP_REFERRAL_STRATEGY = "Referral Strategy";
    public static final String PROP_URL = "Url";
    public static final String PROP_PAGE_SIZE = "Page Size";

    public static final String PROP_USER_SEARCH_BASE = "User Search Base";
    public static final String PROP_USER_OBJECT_CLASS = "User Object Class";
    public static final String PROP_USER_SEARCH_SCOPE = "User Search Scope";
    public static final String PROP_USER_SEARCH_FILTER = "User Search Filter";
    public static final String PROP_USER_IDENTITY_ATTRIBUTE = "User Identity Attribute";
    public static final String PROP_USER_GROUP_ATTRIBUTE = "User Group Name Attribute";
    public static final String PROP_USER_GROUP_REFERENCED_GROUP_ATTRIBUTE = "User Group Name Attribute - Referenced Group Attribute";

    public static final String PROP_GROUP_SEARCH_BASE = "Group Search Base";
    public static final String PROP_GROUP_OBJECT_CLASS = "Group Object Class";
    public static final String PROP_GROUP_SEARCH_SCOPE = "Group Search Scope";
    public static final String PROP_GROUP_SEARCH_FILTER = "Group Search Filter";
    public static final String PROP_GROUP_NAME_ATTRIBUTE = "Group Name Attribute";
    public static final String PROP_GROUP_MEMBER_ATTRIBUTE = "Group Member Attribute";
    public static final String PROP_GROUP_MEMBER_REFERENCED_USER_ATTRIBUTE = "Group Member Attribute - Referenced User Attribute";

    public static final String PROP_SYNC_INTERVAL = "Sync Interval";
    private static final long MINIMUM_SYNC_INTERVAL_MILLISECONDS = 10_000;

    private List<IdentityMapping> identityMappings;
    private List<IdentityMapping> groupMappings;
    private NiFiProperties properties;

    private ScheduledExecutorService ldapSync;
    private AtomicReference<TenantHolder> tenants = new AtomicReference<>(null);

    private String userSearchBase;
    private SearchScope userSearchScope;
    private String userSearchFilter;
    private String userIdentityAttribute;
    private String userObjectClass;
    private String userGroupNameAttribute;
    private String userGroupReferencedGroupAttribute;
    private boolean useDnForUserIdentity;
    private boolean performUserSearch;

    private String groupSearchBase;
    private SearchScope groupSearchScope;
    private String groupSearchFilter;
    private String groupMemberAttribute;
    private String groupMemberReferencedUserAttribute;
    private String groupNameAttribute;
    private String groupObjectClass;
    private boolean useDnForGroupName;
    private boolean performGroupSearch;

    private Integer pageSize;

    @Override
    public void initialize(final UserGroupProviderInitializationContext initializationContext) throws AuthorizerCreationException {
        ldapSync = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            final ThreadFactory factory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(Runnable r) {
                final Thread thread = factory.newThread(r);
                thread.setName(String.format("%s (%s) - background sync thread", getClass().getSimpleName(), initializationContext.getIdentifier()));
                return thread;
            }
        });
    }

    @Override
    public void onConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        final LdapContextSource context = new LdapContextSource();

        final Map<String, Object> baseEnvironment = new HashMap<>();

        // connect/read time out
        setTimeout(configurationContext, baseEnvironment, PROP_CONNECT_TIMEOUT, "com.sun.jndi.ldap.connect.timeout");
        setTimeout(configurationContext, baseEnvironment, PROP_READ_TIMEOUT, "com.sun.jndi.ldap.read.timeout");

        // authentication strategy
        final PropertyValue rawAuthenticationStrategy = configurationContext.getProperty(PROP_AUTHENTICATION_STRATEGY);
        final LdapAuthenticationStrategy authenticationStrategy;
        try {
            authenticationStrategy = LdapAuthenticationStrategy.valueOf(rawAuthenticationStrategy.getValue());
        } catch (final IllegalArgumentException iae) {
            throw new AuthorizerCreationException(String.format("Unrecognized authentication strategy '%s'. Possible values are [%s]",
                    rawAuthenticationStrategy.getValue(), StringUtils.join(LdapAuthenticationStrategy.values(), ", ")));
        }

        switch (authenticationStrategy) {
            case ANONYMOUS:
                context.setAnonymousReadOnly(true);
                break;
            default:
                final String userDn = configurationContext.getProperty(PROP_MANAGER_DN).getValue();
                final String password = configurationContext.getProperty(PROP_MANAGER_PASSWORD).getValue();

                context.setUserDn(userDn);
                context.setPassword(password);

                switch (authenticationStrategy) {
                    case SIMPLE:
                        context.setAuthenticationStrategy(new SimpleDirContextAuthenticationStrategy());
                        break;
                    case LDAPS:
                        context.setAuthenticationStrategy(new SimpleDirContextAuthenticationStrategy());

                        // indicate a secure connection
                        baseEnvironment.put(Context.SECURITY_PROTOCOL, "ssl");

                        // get the configured ssl context
                        final SSLContext ldapsSslContext = getConfiguredSslContext(configurationContext);
                        if (ldapsSslContext != null) {
                            // initialize the ldaps socket factory prior to use
                            LdapsSocketFactory.initialize(ldapsSslContext.getSocketFactory());
                            baseEnvironment.put("java.naming.ldap.factory.socket", LdapsSocketFactory.class.getName());
                        }
                        break;
                    case START_TLS:
                        final AbstractTlsDirContextAuthenticationStrategy tlsAuthenticationStrategy = new DefaultTlsDirContextAuthenticationStrategy();

                        // shutdown gracefully
                        final String rawShutdownGracefully = configurationContext.getProperty("TLS - Shutdown Gracefully").getValue();
                        if (StringUtils.isNotBlank(rawShutdownGracefully)) {
                            final boolean shutdownGracefully = Boolean.TRUE.toString().equalsIgnoreCase(rawShutdownGracefully);
                            tlsAuthenticationStrategy.setShutdownTlsGracefully(shutdownGracefully);
                        }

                        // get the configured ssl context
                        final SSLContext startTlsSslContext = getConfiguredSslContext(configurationContext);
                        if (startTlsSslContext != null) {
                            tlsAuthenticationStrategy.setSslSocketFactory(startTlsSslContext.getSocketFactory());
                        }

                        // set the authentication strategy
                        context.setAuthenticationStrategy(tlsAuthenticationStrategy);
                        break;
                }
                break;
        }

        // referrals
        final String rawReferralStrategy = configurationContext.getProperty(PROP_REFERRAL_STRATEGY).getValue();

        final ReferralStrategy referralStrategy;
        try {
            referralStrategy = ReferralStrategy.valueOf(rawReferralStrategy);
        } catch (final IllegalArgumentException iae) {
            throw new AuthorizerCreationException(String.format("Unrecognized referral strategy '%s'. Possible values are [%s]",
                    rawReferralStrategy, StringUtils.join(ReferralStrategy.values(), ", ")));
        }

        // using the value as this needs to be the lowercase version while the value is configured with the enum constant
        context.setReferral(referralStrategy.getValue());

        // url
        final String urls = configurationContext.getProperty(PROP_URL).getValue();

        if (StringUtils.isBlank(urls)) {
            throw new AuthorizerCreationException("LDAP identity provider 'Url' must be specified.");
        }

        // connection
        context.setUrls(StringUtils.split(urls));

        // raw user search base
        final PropertyValue rawUserSearchBase = configurationContext.getProperty(PROP_USER_SEARCH_BASE);
        final PropertyValue rawUserObjectClass = configurationContext.getProperty(PROP_USER_OBJECT_CLASS);
        final PropertyValue rawUserSearchScope = configurationContext.getProperty(PROP_USER_SEARCH_SCOPE);

        // if loading the users, ensure the object class set
        if (rawUserSearchBase.isSet() && !rawUserObjectClass.isSet()) {
            throw new AuthorizerCreationException("LDAP user group provider 'User Object Class' must be specified when 'User Search Base' is set.");
        }

        // if loading the users, ensure the search scope is set
        if (rawUserSearchBase.isSet() && !rawUserSearchScope.isSet()) {
            throw new AuthorizerCreationException("LDAP user group provider 'User Search Scope' must be specified when 'User Search Base' is set.");
        }

        // user search criteria
        userSearchBase = rawUserSearchBase.getValue();
        userObjectClass = rawUserObjectClass.getValue();
        userSearchFilter = configurationContext.getProperty(PROP_USER_SEARCH_FILTER).getValue();
        userIdentityAttribute = configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE).getValue();
        userGroupNameAttribute = configurationContext.getProperty(PROP_USER_GROUP_ATTRIBUTE).getValue();
        userGroupReferencedGroupAttribute = configurationContext.getProperty(PROP_USER_GROUP_REFERENCED_GROUP_ATTRIBUTE).getValue();

        try {
            userSearchScope = SearchScope.valueOf(rawUserSearchScope.getValue());
        } catch (final IllegalArgumentException iae) {
            throw new AuthorizerCreationException(String.format("Unrecognized user search scope '%s'. Possible values are [%s]",
                    rawUserSearchScope.getValue(), StringUtils.join(SearchScope.values(), ", ")));
        }

        // determine user behavior
        useDnForUserIdentity = StringUtils.isBlank(userIdentityAttribute);
        performUserSearch = StringUtils.isNotBlank(userSearchBase);

        // raw group search criteria
        final PropertyValue rawGroupSearchBase = configurationContext.getProperty(PROP_GROUP_SEARCH_BASE);
        final PropertyValue rawGroupObjectClass = configurationContext.getProperty(PROP_GROUP_OBJECT_CLASS);
        final PropertyValue rawGroupSearchScope = configurationContext.getProperty(PROP_GROUP_SEARCH_SCOPE);

        // if loading the groups, ensure the object class is set
        if (rawGroupSearchBase.isSet() && !rawGroupObjectClass.isSet()) {
            throw new AuthorizerCreationException("LDAP user group provider 'Group Object Class' must be specified when 'Group Search Base' is set.");
        }

        // if loading the groups, ensure the search scope is set
        if (rawGroupSearchBase.isSet() && !rawGroupSearchScope.isSet()) {
            throw new AuthorizerCreationException("LDAP user group provider 'Group Search Scope' must be specified when 'Group Search Base' is set.");
        }

        // group search criteria
        groupSearchBase = rawGroupSearchBase.getValue();
        groupObjectClass = rawGroupObjectClass.getValue();
        groupSearchFilter = configurationContext.getProperty(PROP_GROUP_SEARCH_FILTER).getValue();
        groupNameAttribute = configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE).getValue();
        groupMemberAttribute = configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE).getValue();
        groupMemberReferencedUserAttribute = configurationContext.getProperty(PROP_GROUP_MEMBER_REFERENCED_USER_ATTRIBUTE).getValue();

        try {
            groupSearchScope = SearchScope.valueOf(rawGroupSearchScope.getValue());
        } catch (final IllegalArgumentException iae) {
            throw new AuthorizerCreationException(String.format("Unrecognized group search scope '%s'. Possible values are [%s]",
                    rawGroupSearchScope.getValue(), StringUtils.join(SearchScope.values(), ", ")));
        }

        // determine group behavior
        useDnForGroupName = StringUtils.isBlank(groupNameAttribute);
        performGroupSearch = StringUtils.isNotBlank(groupSearchBase);

        // ensure we are either searching users or groups (at least one must be specified)
        if (!performUserSearch && !performGroupSearch) {
            throw new AuthorizerCreationException("LDAP user group provider 'User Search Base' or 'Group Search Base' must be specified.");
        }

        // ensure group member attribute is set if searching groups but not users
        if (performGroupSearch && !performUserSearch && StringUtils.isBlank(groupMemberAttribute)) {
            throw new AuthorizerCreationException("'Group Member Attribute' is required when searching groups but not users.");
        }

        // ensure that performUserSearch is set when groupMemberReferencedUserAttribute is specified
        if (StringUtils.isNotBlank(groupMemberReferencedUserAttribute) && !performUserSearch) {
            throw new AuthorizerCreationException("''User Search Base' must be set when specifying 'Group Member Attribute - Referenced User Attribute'.");
        }

        // ensure that performGroupSearch is set when userGroupReferencedGroupAttribute is specified
        if (StringUtils.isNotBlank(userGroupReferencedGroupAttribute) && !performGroupSearch) {
            throw new AuthorizerCreationException("'Group Search Base' must be set when specifying 'User Group Name Attribute - Referenced Group Attribute'.");
        }

        // get the page size if configured
        final PropertyValue rawPageSize = configurationContext.getProperty(PROP_PAGE_SIZE);
        if (rawPageSize.isSet() && StringUtils.isNotBlank(rawPageSize.getValue())) {
            pageSize = rawPageSize.asInteger();
        }

        // extract the identity mappings from nifi.properties if any are provided
        identityMappings = Collections.unmodifiableList(IdentityMappingUtil.getIdentityMappings(properties));
        groupMappings = Collections.unmodifiableList(IdentityMappingUtil.getGroupMappings(properties));

        // set the base environment is necessary
        if (!baseEnvironment.isEmpty()) {
            context.setBaseEnvironmentProperties(baseEnvironment);
        }

        try {
            // handling initializing beans
            context.afterPropertiesSet();
        } catch (final Exception e) {
            throw new AuthorizerCreationException(e.getMessage(), e);
        }

        final PropertyValue rawSyncInterval = configurationContext.getProperty(PROP_SYNC_INTERVAL);
        final long syncInterval;
        if (rawSyncInterval.isSet()) {
            try {
                syncInterval = FormatUtils.getTimeDuration(rawSyncInterval.getValue(), TimeUnit.MILLISECONDS);
            } catch (final IllegalArgumentException iae) {
                throw new AuthorizerCreationException(String.format("The %s '%s' is not a valid time duration", PROP_SYNC_INTERVAL, rawSyncInterval.getValue()));
            }
            if (syncInterval < MINIMUM_SYNC_INTERVAL_MILLISECONDS) {
                throw new AuthorizerCreationException(String.format("The %s '%s' is below the minimum value of '%d ms'",
                        PROP_SYNC_INTERVAL, rawSyncInterval.getValue(), MINIMUM_SYNC_INTERVAL_MILLISECONDS));
            }
        } else {
            throw new AuthorizerCreationException(String.format("The '%s' must be specified.", PROP_SYNC_INTERVAL));
        }

        try {
            // perform the initial load, tenants must be loaded as the configured UserGroupProvider is supplied
            // to the AccessPolicyProvider for granting initial permissions
            load(context);

            // ensure the tenants were successfully synced
            if (tenants.get() == null) {
                throw new AuthorizerCreationException("Unable to sync users and groups.");
            }

            // schedule the background thread to load the users/groups
            ldapSync.scheduleWithFixedDelay(() -> {
                try {
                    load(context);
                } catch (final Throwable t) {
                    logger.error("Failed to sync User/Groups from LDAP due to {}. Will try again in {} millis.", new Object[] {t.toString(), syncInterval});
                    if (logger.isDebugEnabled()) {
                        logger.error("", t);
                    }
                }
            }, syncInterval, syncInterval, TimeUnit.MILLISECONDS);
        } catch (final AuthorizationAccessException e) {
            throw new AuthorizerCreationException(e);
        }
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return tenants.get().getAllUsers();
    }

    @Override
    public User getUser(String identifier) throws AuthorizationAccessException {
        return tenants.get().getUsersById().get(identifier);
    }

    @Override
    public User getUserByIdentity(String identity) throws AuthorizationAccessException {
        return tenants.get().getUser(identity);
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return tenants.get().getAllGroups();
    }

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        return tenants.get().getGroupsById().get(identifier);
    }

    @Override
    public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
        final TenantHolder holder = tenants.get();
        return new UserAndGroups() {
            @Override
            public User getUser() {
                return holder.getUser(identity);
            }

            @Override
            public Set<Group> getGroups() {
                return holder.getGroups(identity);
            }
        };
    }

    /**
     * Reloads the tenants.
     */
    private void load(final ContextSource contextSource) {
        // create the ldapTemplate based on the context source. use a single source context to use the same connection
        // to support paging when configured
        final SingleContextSource singleContextSource = new SingleContextSource(contextSource.getReadOnlyContext());
        final LdapTemplate ldapTemplate = new LdapTemplate(singleContextSource);

        try {
            final List<User> userList = new ArrayList<>();
            final List<Group> groupList = new ArrayList<>();

            // group dn -> user identifiers lookup
            final Map<String, Set<String>> groupToUserIdentifierMappings = new HashMap<>();

            // user dn -> user lookup
            final Map<String, User> userLookup = new HashMap<>();

            if (performUserSearch) {
                // search controls
                final SearchControls userControls = new SearchControls();
                userControls.setSearchScope(userSearchScope.ordinal());

                // consider paging support for users
                final DirContextProcessor userProcessor;
                if (pageSize == null) {
                    userProcessor = new NullDirContextProcessor();
                } else {
                    userProcessor = new PagedResultsDirContextProcessor(pageSize);
                }

                // looking for objects matching the user object class
                final AndFilter userFilter = new AndFilter();
                userFilter.and(new EqualsFilter("objectClass", userObjectClass));

                // if a filter has been provided by the user, we add it to the filter
                if (StringUtils.isNotBlank(userSearchFilter)) {
                    userFilter.and(new HardcodedFilter(userSearchFilter));
                }

                do {
                    userList.addAll(ldapTemplate.search(userSearchBase, userFilter.encode(), userControls, new AbstractContextMapper<User>() {
                        @Override
                        protected User doMapFromContext(DirContextOperations ctx) {
                            // get the user identity
                            final String identity = getUserIdentity(ctx);

                            // build the user
                            final User user = new User.Builder().identifierGenerateFromSeed(identity).identity(identity).build();

                            // store the user for group member later
                            userLookup.put(getReferencedUserValue(ctx), user);

                            if (StringUtils.isNotBlank(userGroupNameAttribute)) {
                                final Attribute attributeGroups = ctx.getAttributes().get(userGroupNameAttribute);

                                if (attributeGroups == null) {
                                    logger.warn("User group name attribute [" + userGroupNameAttribute + "] does not exist. Ignoring group membership.");
                                } else {
                                    try {
                                        final NamingEnumeration<String> groupValues = (NamingEnumeration<String>) attributeGroups.getAll();
                                        while (groupValues.hasMoreElements()) {
                                            // store the group -> user identifier mapping
                                            groupToUserIdentifierMappings.computeIfAbsent(groupValues.next(), g -> new HashSet<>()).add(user.getIdentifier());
                                        }
                                    } catch (NamingException e) {
                                        throw new AuthorizationAccessException("Error while retrieving user group name attribute [" + userIdentityAttribute + "].");
                                    }
                                }
                            }

                            return user;
                        }
                    }, userProcessor));
                } while (hasMorePages(userProcessor));
            }

            if (performGroupSearch) {
                final SearchControls groupControls = new SearchControls();
                groupControls.setSearchScope(groupSearchScope.ordinal());

                // consider paging support for groups
                final DirContextProcessor groupProcessor;
                if (pageSize == null) {
                    groupProcessor = new NullDirContextProcessor();
                } else {
                    groupProcessor = new PagedResultsDirContextProcessor(pageSize);
                }

                // looking for objects matching the group object class
                AndFilter groupFilter = new AndFilter();
                groupFilter.and(new EqualsFilter("objectClass", groupObjectClass));

                // if a filter has been provided by the user, we add it to the filter
                if(StringUtils.isNotBlank(groupSearchFilter)) {
                    groupFilter.and(new HardcodedFilter(groupSearchFilter));
                }

                do {
                    groupList.addAll(ldapTemplate.search(groupSearchBase, groupFilter.encode(), groupControls, new AbstractContextMapper<Group>() {
                        @Override
                        protected Group doMapFromContext(DirContextOperations ctx) {
                            final String dn = ctx.getDn().toString();

                            // get the group identity
                            final String name = getGroupName(ctx);

                            // get the value of this group that may associate it to users
                            final String referencedGroupValue = getReferencedGroupValue(ctx);

                            if (!StringUtils.isBlank(groupMemberAttribute)) {
                                Attribute attributeUsers = ctx.getAttributes().get(groupMemberAttribute);
                                if (attributeUsers == null) {
                                    logger.warn("Group member attribute [" + groupMemberAttribute + "] does not exist. Ignoring group membership.");
                                } else {
                                    try {
                                        final NamingEnumeration<String> userValues = (NamingEnumeration<String>) attributeUsers.getAll();
                                        while (userValues.hasMoreElements()) {
                                            final String userValue = userValues.next();

                                            if (performUserSearch) {
                                                // find the user by it's referenced attribute and add the identifier to this group
                                                final User user = userLookup.get(userValue);

                                                // ensure the user is known
                                                if (user != null) {
                                                    groupToUserIdentifierMappings.computeIfAbsent(referencedGroupValue, g -> new HashSet<>()).add(user.getIdentifier());
                                                } else {
                                                    logger.warn(String.format("%s contains member %s but that user was not found while searching users. Ignoring group membership.", name, userValue));
                                                }
                                            } else {
                                                // since performUserSearch is false, then the referenced group attribute must be blank... the user value must be the dn
                                                final String userDn = userValue;

                                                final String userIdentity;
                                                if (useDnForUserIdentity) {
                                                    // use the user value to avoid the unnecessary look up
                                                    userIdentity = userDn;
                                                } else {
                                                    // lookup the user to extract the user identity
                                                    userIdentity = getUserIdentity((DirContextAdapter) ldapTemplate.lookup(userDn));
                                                }

                                                // build the user
                                                final User user = new User.Builder().identifierGenerateFromSeed(userIdentity).identity(userIdentity).build();

                                                // add this user
                                                userList.add(user);
                                                groupToUserIdentifierMappings.computeIfAbsent(referencedGroupValue, g -> new HashSet<>()).add(user.getIdentifier());
                                            }
                                        }
                                    } catch (NamingException e) {
                                        throw new AuthorizationAccessException("Error while retrieving group name attribute [" + groupNameAttribute + "].");
                                    }
                                }
                            }

                            // build this group
                            final Group.Builder groupBuilder = new Group.Builder().identifierGenerateFromSeed(name).name(name);

                            // add all users that were associated with this referenced group attribute
                            if (groupToUserIdentifierMappings.containsKey(referencedGroupValue)) {
                                groupToUserIdentifierMappings.remove(referencedGroupValue).forEach(userIdentifier -> groupBuilder.addUser(userIdentifier));
                            }

                            return groupBuilder.build();
                        }
                    }, groupProcessor));
                } while (hasMorePages(groupProcessor));

                // any remaining groupDn's were referenced by a user but not found while searching groups
                groupToUserIdentifierMappings.forEach((referencedGroupValue, userIdentifiers) -> {
                    logger.warn(String.format("[%s] are members of %s but that group was not found while searching users. Ignoring group membership.",
                            StringUtils.join(userIdentifiers, ", "), referencedGroupValue));
                });
            } else {
                // since performGroupSearch is false, then the referenced user attribute must be blank... the group value must be the dn

                // groups are not being searched so lookup any groups identified while searching users
                groupToUserIdentifierMappings.forEach((groupDn, userIdentifiers) -> {
                    final String groupName;
                    if (useDnForGroupName) {
                        // use the dn to avoid the unnecessary look up
                        groupName = groupDn;
                    } else {
                        groupName = getGroupName((DirContextAdapter) ldapTemplate.lookup(groupDn));
                    }

                    // define the group
                    final Group.Builder groupBuilder = new Group.Builder().identifierGenerateFromSeed(groupName).name(groupName);

                    // add each user
                    userIdentifiers.forEach(userIdentifier -> groupBuilder.addUser(userIdentifier));

                    // build the group
                    groupList.add(groupBuilder.build());
                });
            }

            if (logger.isDebugEnabled()) {
                logger.debug("-------------------------------------");
                logger.debug("Loaded the following users from LDAP:");
                userList.forEach((user) -> logger.debug(" - " + user));
                logger.debug("--------------------------------------");
                logger.debug("Loaded the following groups from LDAP:");
                groupList.forEach((group) -> logger.debug(" - " + group));
                logger.debug("--------------------------------------");
            }

            // record the updated tenants
            tenants.set(new TenantHolder(new HashSet<>(userList), new HashSet<>(groupList)));
        } finally {
            singleContextSource.destroy();
        }
    }

    private boolean hasMorePages(final DirContextProcessor processor ) {
        return processor instanceof PagedResultsDirContextProcessor && ((PagedResultsDirContextProcessor) processor).hasMore();
    }

    private String getUserIdentity(final DirContextOperations ctx) {
        final String identity;

        if (useDnForUserIdentity) {
            identity = ctx.getDn().toString();
        } else {
            final Attribute attributeName = ctx.getAttributes().get(userIdentityAttribute);
            if (attributeName == null) {
                throw new AuthorizationAccessException("User identity attribute [" + userIdentityAttribute + "] does not exist.");
            }

            try {
                identity = (String) attributeName.get();
            } catch (NamingException e) {
                throw new AuthorizationAccessException("Error while retrieving user name attribute [" + userIdentityAttribute + "].");
            }
        }

        return IdentityMappingUtil.mapIdentity(identity, identityMappings);
    }

    private String getReferencedUserValue(final DirContextOperations ctx) {
        final String referencedUserValue;

        if (StringUtils.isBlank(groupMemberReferencedUserAttribute)) {
            referencedUserValue = ctx.getDn().toString();
        } else {
            final Attribute attributeName = ctx.getAttributes().get(groupMemberReferencedUserAttribute);
            if (attributeName == null) {
                throw new AuthorizationAccessException("Referenced user value attribute [" + groupMemberReferencedUserAttribute + "] does not exist.");
            }

            try {
                referencedUserValue = (String) attributeName.get();
            } catch (NamingException e) {
                throw new AuthorizationAccessException("Error while retrieving reference user value attribute [" + groupMemberReferencedUserAttribute + "].");
            }
        }

        return referencedUserValue;
    }

    private String getGroupName(final DirContextOperations ctx) {
        final String name;

        if (useDnForGroupName) {
            name = ctx.getDn().toString();
        } else {
            final Attribute attributeName = ctx.getAttributes().get(groupNameAttribute);
            if (attributeName == null) {
                throw new AuthorizationAccessException("Group identity attribute [" + groupNameAttribute + "] does not exist.");
            }

            try {
                name = (String) attributeName.get();
            } catch (NamingException e) {
                throw new AuthorizationAccessException("Error while retrieving group name attribute [" + groupNameAttribute + "].");
            }
        }

        return IdentityMappingUtil.mapIdentity(name, groupMappings);
    }

    private String getReferencedGroupValue(final DirContextOperations ctx) {
        final String referencedGroupValue;

        if (StringUtils.isBlank(userGroupReferencedGroupAttribute)) {
            referencedGroupValue = ctx.getDn().toString();
        } else {
            final Attribute attributeName = ctx.getAttributes().get(userGroupReferencedGroupAttribute);
            if (attributeName == null) {
                throw new AuthorizationAccessException("Referenced group value attribute [" + userGroupReferencedGroupAttribute + "] does not exist.");
            }

            try {
                referencedGroupValue = (String) attributeName.get();
            } catch (NamingException e) {
                throw new AuthorizationAccessException("Error while retrieving referenced group value attribute [" + userGroupReferencedGroupAttribute + "].");
            }
        }

        return referencedGroupValue;
    }

    @AuthorizerContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public final void preDestruction() throws ProviderDestructionException {
        ldapSync.shutdown();
        try {
            if (!ldapSync.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
                logger.info("Failed to stop ldap sync thread in 10 sec. Terminating");
                ldapSync.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void setTimeout(final AuthorizerConfigurationContext configurationContext,
                            final Map<String, Object> baseEnvironment,
                            final String configurationProperty,
                            final String environmentKey) {

        final PropertyValue rawTimeout = configurationContext.getProperty(configurationProperty);
        if (rawTimeout.isSet()) {
            try {
                final Long timeout = FormatUtils.getTimeDuration(rawTimeout.getValue(), TimeUnit.MILLISECONDS);
                baseEnvironment.put(environmentKey, timeout.toString());
            } catch (final IllegalArgumentException iae) {
                throw new AuthorizerCreationException(String.format("The %s '%s' is not a valid time duration", configurationProperty, rawTimeout));
            }
        }
    }

    private SSLContext getConfiguredSslContext(final AuthorizerConfigurationContext configurationContext) {
        final String rawKeystore = configurationContext.getProperty("TLS - Keystore").getValue();
        final String rawKeystorePassword = configurationContext.getProperty("TLS - Keystore Password").getValue();
        final String rawKeystoreType = configurationContext.getProperty("TLS - Keystore Type").getValue();
        final String rawTruststore = configurationContext.getProperty("TLS - Truststore").getValue();
        final String rawTruststorePassword = configurationContext.getProperty("TLS - Truststore Password").getValue();
        final String rawTruststoreType = configurationContext.getProperty("TLS - Truststore Type").getValue();
        final String rawClientAuth = configurationContext.getProperty("TLS - Client Auth").getValue();
        final String rawProtocol = configurationContext.getProperty("TLS - Protocol").getValue();

        // create the ssl context
        final SSLContext sslContext;
        try {
            if (StringUtils.isBlank(rawKeystore) && StringUtils.isBlank(rawTruststore)) {
                sslContext = null;
            } else {
                // ensure the protocol is specified
                if (StringUtils.isBlank(rawProtocol)) {
                    throw new AuthorizerCreationException("TLS - Protocol must be specified.");
                }

                if (StringUtils.isBlank(rawKeystore)) {
                    sslContext = SslContextFactory.createTrustSslContext(rawTruststore, rawTruststorePassword.toCharArray(), rawTruststoreType, rawProtocol);
                } else if (StringUtils.isBlank(rawTruststore)) {
                    sslContext = SslContextFactory.createSslContext(rawKeystore, rawKeystorePassword.toCharArray(), rawKeystoreType, rawProtocol);
                } else {
                    // determine the client auth if specified
                    final ClientAuth clientAuth;
                    if (StringUtils.isBlank(rawClientAuth)) {
                        clientAuth = ClientAuth.NONE;
                    } else {
                        try {
                            clientAuth = ClientAuth.valueOf(rawClientAuth);
                        } catch (final IllegalArgumentException iae) {
                            throw new AuthorizerCreationException(String.format("Unrecognized client auth '%s'. Possible values are [%s]",
                                    rawClientAuth, StringUtils.join(ClientAuth.values(), ", ")));
                        }
                    }

                    sslContext = SslContextFactory.createSslContext(rawKeystore, rawKeystorePassword.toCharArray(), rawKeystoreType,
                            rawTruststore, rawTruststorePassword.toCharArray(), rawTruststoreType, clientAuth, rawProtocol);
                }
            }
        } catch (final KeyStoreException | NoSuchAlgorithmException | CertificateException | UnrecoverableKeyException | KeyManagementException | IOException e) {
            throw new AuthorizerCreationException(e.getMessage(), e);
        }

        return sslContext;
    }

}
