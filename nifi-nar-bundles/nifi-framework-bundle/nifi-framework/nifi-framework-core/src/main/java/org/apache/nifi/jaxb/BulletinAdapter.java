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
package org.apache.nifi.jaxb;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.reporting.Bulletin;

/**
 */
public class BulletinAdapter extends XmlAdapter<AdaptedBulletin, Bulletin> {

    @Override
    public Bulletin unmarshal(final AdaptedBulletin b) throws Exception {
        if (b == null) {
            return null;
        }
        // TODO - timestamp is overridden here with a new timestamp... address?
        if (b.getSourceId() == null) {
            return BulletinFactory.createBulletin(b.getCategory(), b.getLevel(), b.getMessage());
        } else {
            return BulletinFactory.createBulletin(b.getGroupId(), b.getGroupName(), b.getSourceId(), b.getSourceType(),
                    b.getSourceName(), b.getCategory(), b.getLevel(), b.getMessage());
        }
    }

    @Override
    public AdaptedBulletin marshal(final Bulletin b) throws Exception {
        if (b == null) {
            return null;
        }
        final AdaptedBulletin aBulletin = new AdaptedBulletin();
        aBulletin.setId(b.getId());
        aBulletin.setTimestamp(b.getTimestamp());
        aBulletin.setGroupId(b.getGroupId());
        aBulletin.setGroupName(b.getGroupName());
        aBulletin.setSourceId(b.getSourceId());
        aBulletin.setSourceType(b.getSourceType());
        aBulletin.setSourceName(b.getSourceName());
        aBulletin.setCategory(b.getCategory());
        aBulletin.setLevel(b.getLevel());
        aBulletin.setMessage(b.getMessage());
        return aBulletin;
    }

}
