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

import javax.security.auth.login.LoginException;
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
     * @throws LoginException if the login fails
     */
    void login() throws LoginException;

    /**
     * Performs a logout for the given user.
     *
     * @throws LoginException if the logout fails
     */
    void logout() throws LoginException;

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
     * @param <T> the type of response
     * @return the result of the action
     * @throws IllegalStateException if attempting to execute an action before performing a login
     * @throws PrivilegedActionException if the action itself threw an exception
     */
    <T> T doAs(PrivilegedExceptionAction<T> action)
            throws IllegalStateException, PrivilegedActionException;

    /**
     * Performs a re-login if the TGT is close to expiration.
     *
     * @return true if a relogin was performed, false otherwise
     * @throws LoginException if the relogin fails
     */
    boolean checkTGTAndRelogin() throws LoginException;

    /**
     * @return true if this user is currently logged in, false otherwise
     */
    boolean isLoggedIn();

    /**
     * @return the principal for this user
     */
    String getPrincipal();

}
