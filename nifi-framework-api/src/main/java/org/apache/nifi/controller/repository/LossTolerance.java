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
package org.apache.nifi.controller.repository;

/**
 * Indicates whether data may be lost without significant consequence. A repository is free to trade durability for
 * performance when handling data that is marked as {@link #LOSS_TOLERANT}, for example by using more volatile storage.
 */
public enum LossTolerance {
    LOSS_TOLERANT(true),
    LOSS_INTOLERANT(false);

    private final boolean lossTolerant;

    LossTolerance(final boolean lossTolerant) {
        this.lossTolerant = lossTolerant;
    }

    public boolean isLossTolerant() {
        return lossTolerant;
    }
}
