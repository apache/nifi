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

import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.BulletinRepository;

import java.util.List;

public class MockBulletinRepository implements BulletinRepository {

    @Override
    public void addBulletin(Bulletin bulletin) {
        // TODO: Implement

    }

    @Override
    public int getControllerBulletinCapacity() {
        // TODO: Implement
        return 0;
    }

    @Override
    public int getComponentBulletinCapacity() {
        // TODO: Implement
        return 0;
    }

    @Override
    public List<Bulletin> findBulletins(BulletinQuery bulletinQuery) {
        // TODO: Implement
        return null;
    }

    @Override
    public List<Bulletin> findBulletinsForSource(String sourceId) {
        // TODO: Implement
        return null;
    }

    @Override
    public List<Bulletin> findBulletinsForGroupBySource(String groupId) {
        // TODO: Implement
        return null;
    }

    @Override
    public List<Bulletin> findBulletinsForGroupBySource(String groupId, int maxPerComponent) {
        // TODO: Implement
        return null;
    }

    @Override
    public List<Bulletin> findBulletinsForController() {
        // TODO: Implement
        return null;
    }

    @Override
    public List<Bulletin> findBulletinsForController(int max) {
        // TODO: Implement
        return null;
    }
}
