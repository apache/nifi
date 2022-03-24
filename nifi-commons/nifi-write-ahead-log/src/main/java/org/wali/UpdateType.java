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
package org.wali;

/**
 * <p>
 * Enumerates the valid types of things that can cause a
 * {@link WriteAheadRepository} to update its state</p>
 */
public enum UpdateType {

    /**
     * Used when a new Record has been created
     */
    CREATE,
    /**
     * Used when a Record has been updated in some way
     */
    UPDATE,
    /**
     * Used to indicate that a Record has been deleted and should be removed
     * from the Repository
     */
    DELETE,
    /**
     * Used to indicate that a Record still exists but has been moved elsewhere,
     * so that it is no longer maintained by the WALI instance
     */
    SWAP_OUT,
    /**
     * Used to indicate that a Record that was previously Swapped Out is now
     * being Swapped In
     */
    SWAP_IN;
}
