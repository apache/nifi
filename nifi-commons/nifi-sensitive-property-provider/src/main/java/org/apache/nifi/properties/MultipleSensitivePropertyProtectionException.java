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
package org.apache.nifi.properties;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

public class MultipleSensitivePropertyProtectionException extends SensitivePropertyProtectionException {

    private Set<String> failedKeys;

    /**
     * Constructs a new throwable with {@code null} as its detail message.
     * The cause is not initialized, and may subsequently be initialized by a
     * call to {@link #initCause}.
     * <p>
     * <p>The {@link #fillInStackTrace()} method is called to initialize
     * the stack trace data in the newly created throwable.
     */
    public MultipleSensitivePropertyProtectionException() {
    }

    /**
     * Constructs a new throwable with the specified detail message.  The
     * cause is not initialized, and may subsequently be initialized by
     * a call to {@link #initCause}.
     * <p>
     * <p>The {@link #fillInStackTrace()} method is called to initialize
     * the stack trace data in the newly created throwable.
     *
     * @param message the detail message. The detail message is saved for
     *                later retrieval by the {@link #getMessage()} method.
     */
    public MultipleSensitivePropertyProtectionException(String message) {
        super(message);
    }

    /**
     * Constructs a new throwable with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this throwable's detail message.
     * <p>
     * <p>The {@link #fillInStackTrace()} method is called to initialize
     * the stack trace data in the newly created throwable.
     *
     * @param message the detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the
     *                {@link #getCause()} method).  (A {@code null} value is
     *                permitted, and indicates that the cause is nonexistent or
     *                unknown.)
     * @since 1.4
     */
    public MultipleSensitivePropertyProtectionException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new throwable with the specified cause and a detail
     * message of {@code (cause==null ? null : cause.toString())} (which
     * typically contains the class and detail message of {@code cause}).
     * This constructor is useful for throwables that are little more than
     * wrappers for other throwables (for example, PrivilegedActionException).
     * <p>
     * <p>The {@link #fillInStackTrace()} method is called to initialize
     * the stack trace data in the newly created throwable.
     *
     * @param cause the cause (which is saved for later retrieval by the
     *              {@link #getCause()} method).  (A {@code null} value is
     *              permitted, and indicates that the cause is nonexistent or
     *              unknown.)
     * @since 1.4
     */
    public MultipleSensitivePropertyProtectionException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with the provided message and a unique set of the keys that caused the error.
     *
     * @param message    the message
     * @param failedKeys any failed keys
     */
    public MultipleSensitivePropertyProtectionException(String message, Collection<String> failedKeys) {
        this(message, failedKeys, null);
    }

    /**
     * Constructs a new exception with the provided message and a unique set of the keys that caused the error.
     *
     * @param message    the message
     * @param failedKeys any failed keys
     * @param cause      the cause (which is saved for later retrieval by the
     *                   {@link #getCause()} method).  (A {@code null} value is
     *                   permitted, and indicates that the cause is nonexistent or
     *                   unknown.)
     */
    public MultipleSensitivePropertyProtectionException(String message, Collection<String> failedKeys, Throwable cause) {
        super(message, cause);
        this.failedKeys = new HashSet<>(failedKeys);
    }

    public Set<String> getFailedKeys() {
        return this.failedKeys;
    }

    @Override
    public String toString() {
        return "SensitivePropertyProtectionException for [" + StringUtils.join(this.failedKeys, ", ") + "]: " + getLocalizedMessage();
    }
}
