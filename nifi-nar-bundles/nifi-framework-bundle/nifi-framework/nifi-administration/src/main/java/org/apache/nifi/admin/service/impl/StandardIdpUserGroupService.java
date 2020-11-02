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

import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.admin.service.IdpUserGroupService;
import org.apache.nifi.admin.service.action.CreateIdpUserGroup;
import org.apache.nifi.admin.service.action.CreateIdpUserGroups;
import org.apache.nifi.admin.service.action.DeleteIdpUserGroupsByIdentity;
import org.apache.nifi.admin.service.action.GetIdpUserGroupsByIdentity;
import org.apache.nifi.admin.service.transaction.Transaction;
import org.apache.nifi.admin.service.transaction.TransactionBuilder;
import org.apache.nifi.admin.service.transaction.TransactionException;
import org.apache.nifi.idp.IdpType;
import org.apache.nifi.idp.IdpUserGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StandardIdpUserGroupService implements IdpUserGroupService {

    private static Logger LOGGER = LoggerFactory.getLogger(StandardIdpUserGroupService.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    private TransactionBuilder transactionBuilder;

    @Override
    public IdpUserGroup createUserGroup(final IdpUserGroup userGroup) {
        Transaction transaction = null;
        IdpUserGroup createdUserGroup;

        writeLock.lock();
        try {
            // ensure the created date is set
            if (userGroup.getCreated() == null) {
                userGroup.setCreated(new Date());
            }

            // start the transaction
            transaction = transactionBuilder.start();

            // create the user group
            final CreateIdpUserGroup action = new CreateIdpUserGroup(userGroup);
            createdUserGroup = transaction.execute(action);

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

        return createdUserGroup;
    }

    @Override
    public List<IdpUserGroup> createUserGroups(final List<IdpUserGroup> userGroups) {
        Transaction transaction = null;
        List<IdpUserGroup> createdUserGroups;

        writeLock.lock();
        try {
            // ensure the created date is set
            for (final IdpUserGroup userGroup : userGroups) {
                if (userGroup.getCreated() == null) {
                    userGroup.setCreated(new Date());
                }
            }

            // start the transaction
            transaction = transactionBuilder.start();

            // create the user group
            final CreateIdpUserGroups action = new CreateIdpUserGroups(userGroups);
            createdUserGroups = transaction.execute(action);

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

        return createdUserGroups;
    }

    @Override
    public List<IdpUserGroup> getUserGroups(final String identity) {
        Transaction transaction = null;
        List<IdpUserGroup> userGroups;

        readLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // get the user groups
            final GetIdpUserGroupsByIdentity action = new GetIdpUserGroupsByIdentity(identity);
            userGroups = transaction.execute(action);

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
            readLock.unlock();
        }

        return userGroups;
    }

    @Override
    public void deleteUserGroups(final String identity) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // delete the credential
            final DeleteIdpUserGroupsByIdentity action = new DeleteIdpUserGroupsByIdentity(identity);
            Integer rowsDeleted = transaction.execute(action);
            LOGGER.debug("Deleted {} user groups for identity {}", rowsDeleted, identity);

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
    public List<IdpUserGroup> replaceUserGroups(final String userIdentity, final IdpType idpType, final Set<String> groupNames) {
        Transaction transaction = null;
        List<IdpUserGroup> createdUserGroups;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // delete the existing groups
            final DeleteIdpUserGroupsByIdentity deleteAction = new DeleteIdpUserGroupsByIdentity(userIdentity);
            Integer rowsDeleted = transaction.execute(deleteAction);
            LOGGER.debug("Deleted {} user groups for identity {}", rowsDeleted, userIdentity);

            // create the user groups
            final List<IdpUserGroup> idpUserGroups = new ArrayList<>();
            for (final String groupName : groupNames) {
                final IdpUserGroup idpUserGroup = new IdpUserGroup();
                idpUserGroup.setIdentity(userIdentity);
                idpUserGroup.setType(idpType);
                idpUserGroup.setGroupName(groupName);
                idpUserGroup.setCreated(new Date());
                idpUserGroups.add(idpUserGroup);
                LOGGER.debug("{} belongs to {}", userIdentity, groupName);
            }

            final CreateIdpUserGroups createAction = new CreateIdpUserGroups(idpUserGroups);
            createdUserGroups = transaction.execute(createAction);

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

        return createdUserGroups;
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

}
