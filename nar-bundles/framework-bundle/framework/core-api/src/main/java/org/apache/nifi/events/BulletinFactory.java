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
package org.apache.nifi.events;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.reporting.Bulletin;

/**
 *
 */
public final class BulletinFactory {

    private static final AtomicLong currentId = new AtomicLong(0);

    public static Bulletin createBulletin(final Connectable connectable, final String category, final String severity, final String message) {
        return BulletinFactory.createBulletin(connectable.getProcessGroup().getIdentifier(), connectable.getIdentifier(), connectable.getName(), category, severity, message);
    }

    public static Bulletin createBulletin(final String groupId, final String sourceId, final String sourceName, final String category, final String severity, final String message) {
        final Bulletin bulletin = new ComponentBulletin(currentId.getAndIncrement());
        bulletin.setGroupId(groupId);
        bulletin.setSourceId(sourceId);
        bulletin.setSourceName(sourceName);
        bulletin.setCategory(category);
        bulletin.setLevel(severity);
        bulletin.setMessage(message);
        return bulletin;
    }

    public static Bulletin createBulletin(final String category, final String severity, final String message) {
        final Bulletin bulletin = new SystemBulletin(currentId.getAndIncrement());
        bulletin.setCategory(category);
        bulletin.setLevel(severity);
        bulletin.setMessage(message);
        return bulletin;
    }
}
