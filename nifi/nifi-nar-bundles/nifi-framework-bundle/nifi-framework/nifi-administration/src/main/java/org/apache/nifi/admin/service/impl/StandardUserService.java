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
package org.apache.nifi.admin.service.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.service.AccountDisabledException;
import org.apache.nifi.admin.service.AccountPendingException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.admin.service.action.AuthorizeDownloadAction;
import org.apache.nifi.admin.service.action.AuthorizeUserAction;
import org.apache.nifi.admin.service.action.DeleteUserAction;
import org.apache.nifi.admin.service.action.DisableUserAction;
import org.apache.nifi.admin.service.action.DisableUserGroupAction;
import org.apache.nifi.admin.service.action.FindUserByDnAction;
import org.apache.nifi.admin.service.action.FindUserByIdAction;
import org.apache.nifi.admin.service.action.GetUserGroupAction;
import org.apache.nifi.admin.service.action.GetUsersAction;
import org.apache.nifi.admin.service.action.HasPendingUserAccounts;
import org.apache.nifi.admin.service.action.InvalidateUserAccountAction;
import org.apache.nifi.admin.service.action.InvalidateUserGroupAccountsAction;
import org.apache.nifi.admin.service.action.RequestUserAccountAction;
import org.apache.nifi.admin.service.action.SeedUserAccountsAction;
import org.apache.nifi.admin.service.action.UpdateUserAction;
import org.apache.nifi.admin.service.action.UpdateUserGroupAction;
import org.apache.nifi.admin.service.action.UngroupUserAction;
import org.apache.nifi.admin.service.action.UngroupUserGroupAction;
import org.apache.nifi.admin.service.transaction.Transaction;
import org.apache.nifi.admin.service.transaction.TransactionBuilder;
import org.apache.nifi.admin.service.transaction.TransactionException;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.authorization.DownloadAuthorization;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.user.NiFiUserGroup;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StandardUserService implements UserService {

    private static final Logger logger = LoggerFactory.getLogger(StandardUserService.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    private TransactionBuilder transactionBuilder;
    private NiFiProperties properties;

    /**
     * Seed any users from the authority provider that are not already present.
     */
    public void seedUserAccounts() {
        // do not seed node's user cache. when/if the node disconnects its
        // cache will be populated lazily (as needed)
        if (properties.isNode()) {
            return;
        }

        Transaction transaction = null;
        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // seed the accounts
            SeedUserAccountsAction seedUserAccounts = new SeedUserAccountsAction();
            transaction.execute(seedUserAccounts);

            // commit the transaction
            transaction.commit();
        } catch (AdministrationException ae) {
            rollback(transaction);
            throw ae;
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    @Override
    public NiFiUser createPendingUserAccount(String dn, String justification) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // create the account request
            RequestUserAccountAction requestUserAccount = new RequestUserAccountAction(dn, justification);
            NiFiUser user = transaction.execute(requestUserAccount);

            // commit the transaction
            transaction.commit();

            // return the nifi user
            return user;
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    @Override
    public NiFiUserGroup updateGroup(final String group, final Set<String> userIds, final Set<Authority> authorities) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // if user ids have been specified, invalidate the user accounts before performing
            // the desired updates. if case of an error, this will ensure that these users are
            // authorized the next time the access the application
            if (userIds != null) {
                for (final String userId : userIds) {
                    invalidateUserAccount(userId);
                }
            }

            // start the transaction
            transaction = transactionBuilder.start();

            // set the authorities for each user in this group if specified
            final UpdateUserGroupAction updateUserGroup = new UpdateUserGroupAction(group, userIds, authorities);
            transaction.execute(updateUserGroup);

            // get all the users that are now in this group
            final GetUserGroupAction getUserGroup = new GetUserGroupAction(group);
            final NiFiUserGroup userGroup = transaction.execute(getUserGroup);

            // commit the transaction
            transaction.commit();

            return userGroup;
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    @Override
    public void ungroupUser(String id) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // ungroup the specified user
            final UngroupUserAction ungroupUser = new UngroupUserAction(id);
            transaction.execute(ungroupUser);

            // commit the transaction
            transaction.commit();
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    @Override
    public void ungroup(String group) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // ungroup the specified user
            final UngroupUserGroupAction ungroupUserGroup = new UngroupUserGroupAction(group);
            transaction.execute(ungroupUserGroup);

            // commit the transaction
            transaction.commit();
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    @Override
    public NiFiUser checkAuthorization(String dn) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // create the connection
            transaction = transactionBuilder.start();

            // determine how long the cache is valid for
            final int cacheSeconds;
            try {
                cacheSeconds = (int) FormatUtils.getTimeDuration(properties.getUserCredentialCacheDuration(), TimeUnit.SECONDS);
            } catch (IllegalArgumentException iae) {
                throw new AdministrationException("User credential cache duration is not configured correctly.");
            }

            // attempt to authorize the user
            AuthorizeUserAction authorizeUser = new AuthorizeUserAction(dn, cacheSeconds);
            NiFiUser user = transaction.execute(authorizeUser);

            // commit the transaction
            transaction.commit();

            // return the nifi user
            return user;
        } catch (DataAccessException | TransactionException dae) {
            rollback(transaction);
            throw new AdministrationException(dae);
        } catch (AccountDisabledException | AccountPendingException ade) {
            rollback(transaction);
            throw ade;
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    @Override
    public void deleteUser(String id) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // create the connection
            transaction = transactionBuilder.start();

            // delete the user
            DeleteUserAction deleteUser = new DeleteUserAction(id);
            transaction.execute(deleteUser);

            // commit the transaction
            transaction.commit();
        } catch (DataAccessException | TransactionException dae) {
            rollback(transaction);
            throw new AdministrationException(dae);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    @Override
    public NiFiUser disable(String id) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // create the connection
            transaction = transactionBuilder.start();

            // disable the user
            DisableUserAction disableUser = new DisableUserAction(id);
            NiFiUser user = transaction.execute(disableUser);

            // commit the transaction
            transaction.commit();

            // return the user
            return user;
        } catch (DataAccessException | TransactionException dae) {
            rollback(transaction);
            throw new AdministrationException(dae);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    @Override
    public NiFiUserGroup disableGroup(String group) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // create the connection
            transaction = transactionBuilder.start();

            // disable the user
            DisableUserGroupAction disableUser = new DisableUserGroupAction(group);
            NiFiUserGroup userGroup = transaction.execute(disableUser);

            // commit the transaction
            transaction.commit();

            // return the user
            return userGroup;
        } catch (DataAccessException | TransactionException dae) {
            rollback(transaction);
            throw new AdministrationException(dae);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    @Override
    public NiFiUser update(String id, Set<Authority> authorities) {
        Transaction transaction = null;

        // may be empty but not null
        if (authorities == null) {
            throw new IllegalArgumentException("The specified authorities cannot be null.");
        }

        writeLock.lock();
        try {
            // invalidate the user account in preparation for potential subsequent errors
            invalidateUserAccount(id);

            // at this point the current user account has been invalidated so we will
            // attempt to update the account. if any part fails we are assured the
            // user will be need to be given approval before they access the system at
            // a later time
            // start the transaction
            transaction = transactionBuilder.start();

            // update the user authorities
            UpdateUserAction setUserAuthorities = new UpdateUserAction(id, authorities);
            NiFiUser user = transaction.execute(setUserAuthorities);

            // commit the transaction
            transaction.commit();

            // return the user
            return user;
        } catch (TransactionException | DataAccessException e) {
            rollback(transaction);
            throw new AdministrationException(e);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    /**
     * Invalidates the user with the specified id. This is done to ensure a user
     * account will need to be re-validated in case an error occurs while
     * modifying a user account. This method should only be invoked from within
     * a write lock.
     *
     * @param id user account identifier
     */
    @Override
    public void invalidateUserAccount(String id) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // invalidate the user account
            InvalidateUserAccountAction invalidateUserAccount = new InvalidateUserAccountAction(id);
            transaction.execute(invalidateUserAccount);

            // commit the transaction
            transaction.commit();
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    @Override
    public void invalidateUserGroupAccount(String group) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // invalidate the user account
            InvalidateUserGroupAccountsAction invalidateUserGroupAccounts = new InvalidateUserGroupAccountsAction(group);
            transaction.execute(invalidateUserGroupAccounts);

            // commit the transaction
            transaction.commit();
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    // -----------------
    // read only methods
    // -----------------
    @Override
    public Boolean hasPendingUserAccount() {
        Transaction transaction = null;

        readLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            final HasPendingUserAccounts hasPendingAccounts = new HasPendingUserAccounts();
            final Boolean hasPendingUserAccounts = transaction.execute(hasPendingAccounts);

            // commit the transaction
            transaction.commit();

            return hasPendingUserAccounts;
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            readLock.unlock();
        }
    }

    @Override
    public DownloadAuthorization authorizeDownload(final List<String> dnChain, final Map<String, String> attributes) {
        Transaction transaction = null;

        readLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // authorize the download
            AuthorizeDownloadAction authorizeDownload = new AuthorizeDownloadAction(dnChain, attributes);
            DownloadAuthorization downloadAuthorization = transaction.execute(authorizeDownload);

            // commit the transaction
            transaction.commit();

            // return the authorization
            return downloadAuthorization;
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            readLock.unlock();
        }
    }

    @Override
    public Collection<NiFiUser> getUsers() {
        Transaction transaction = null;

        readLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // get all users
            GetUsersAction getUsers = new GetUsersAction();
            Collection<NiFiUser> users = transaction.execute(getUsers);

            // commit the transaction
            transaction.commit();

            // return the users
            return users;
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            readLock.unlock();
        }
    }

    @Override
    public NiFiUser getUserById(String id) {
        Transaction transaction = null;

        readLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // return the desired user
            FindUserByIdAction findUserById = new FindUserByIdAction(id);
            NiFiUser user = transaction.execute(findUserById);

            // commit the transaction
            transaction.commit();

            // return the user
            return user;
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            readLock.unlock();
        }
    }

    @Override
    public NiFiUser getUserByDn(String dn) {
        Transaction transaction = null;

        readLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // return the desired user
            FindUserByDnAction findUserByDn = new FindUserByDnAction(dn);
            NiFiUser user = transaction.execute(findUserByDn);

            // commit the transaction
            transaction.commit();

            // return the user
            return user;
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            readLock.unlock();
        }
    }

    private void rollback(final Transaction transaction) {
        if (transaction != null) {
            transaction.rollback();
        }
    }

    private void closeQuietly(final Transaction transaction) {
        if (transaction != null) {
            try {
                transaction.close();
            } catch (final IOException ioe) {
            }
        }
    }

    public void setTransactionBuilder(TransactionBuilder transactionBuilder) {
        this.transactionBuilder = transactionBuilder;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

}
