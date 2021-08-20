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
package org.apache.nifi.security.krb;

import javax.security.auth.login.AppConfigurationEntry;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * A keytab-based user that can login/logout and perform actions as the given user.
 */
public interface KerberosUser {

    /**
     * Performs a login for the given user.
     *
     * @throws KerberosLoginException if the login fails
     */
    void login();

    /**
     * Performs a logout for the given user.
     *
     * @throws KerberosLoginException if the logout fails
     */
    void logout();

    /**
     * Executes the given action as the given user.
     *
     * @param action the action to execute
     * @param <T> the type of response
     * @return the result of the action
     * @throws IllegalStateException if attempting to execute an action before performing a login
     */
    <T> T doAs(PrivilegedAction<T> action) throws IllegalStateException;

    /**
     * Executes the given action as the given user.
     *
     * @param action the action to execute
     * @param contextClassLoader the class loader to set as the current thread's context class loader
     * @param <T> the type of response
     * @return the result of the action
     * @throws IllegalStateException if attempting to execute an action before performing a login
     */
    default <T> T doAs(PrivilegedAction<T> action, ClassLoader contextClassLoader) throws IllegalStateException {
        final ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(contextClassLoader);
        try {
            return doAs(action);
        } finally {
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        }
    }

    /**
     * Executes the given action as the given user.
     *
     * @param action the action to execute
     * @param <T> the type of response
     * @return the result of the action
     * @throws IllegalStateException if attempting to execute an action before performing a login
     * @throws PrivilegedActionException if the action itself threw an exception
     */
    <T> T doAs(PrivilegedExceptionAction<T> action)
            throws IllegalStateException, PrivilegedActionException;

    /**
     * Executes the given action as the given user.
     *
     * @param action the action to execute
     * @param contextClassLoader the class loader to set as the current thread's context class loader
     * @param <T> the type of response
     * @return the result of the action
     * @throws IllegalStateException if attempting to execute an action before performing a login
     * @throws PrivilegedActionException if the action itself threw an exception
     */
    default <T> T doAs(PrivilegedExceptionAction<T> action, ClassLoader contextClassLoader)
            throws IllegalStateException, PrivilegedActionException {
        final ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(contextClassLoader);
        try {
            return doAs(action);
        } finally {
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        }
    }

    /**
     * Performs a re-login if the TGT is close to expiration.
     *
     * @return true if a relogin was performed, false otherwise
     * @throws KerberosLoginException if the relogin fails
     */
    boolean checkTGTAndRelogin();

    /**
     * @return true if this user is currently logged in, false otherwise
     */
    boolean isLoggedIn();

    /**
     * @return the principal for this user
     */
    String getPrincipal();

    /**
     * @return the configuration entry used to perform the login
     */
    AppConfigurationEntry getConfigurationEntry();

}
