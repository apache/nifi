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
package org.apache.nifi.util;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @deprecated As of release 1.0.1. Please use {@link AtomicBoolean}
 *
 * Wraps an Boolean value so that it can be declared <code>final</code> and still be accessed from inner classes;
 * the functionality is similar to that of an AtomicBoolean, but operations on this class
 * are not atomic. This results in greater performance when the atomicity is not needed.
 *
 */

@Deprecated
public class BooleanHolder extends ObjectHolder<Boolean> {

    public BooleanHolder(final boolean initialValue) {
        super(initialValue);
    }

}
