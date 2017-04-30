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

package org.apache.nifi.processor.util.list;

import java.util.Collection;
import java.util.Date;

import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

/**
 * A simple POJO for maintaining state about the last entities listed by an AbstractListProcessor that was performed so that
 * we can avoid pulling the same file multiple times
 */
@XmlType(name = "listing")
public class EntityListing {

    private Date latestTimestamp;
    private Collection<String> matchingIdentifiers;

    /**
     * @return the modification date of the newest file that was contained in the listing
     */
    public Date getLatestTimestamp() {
        return latestTimestamp;
    }

    /**
     * Sets the timestamp of the modification date of the newest file that was contained in the listing
     *
     * @param latestTimestamp the timestamp of the modification date of the newest file that was contained in the listing
     */
    public void setLatestTimestamp(Date latestTimestamp) {
        this.latestTimestamp = latestTimestamp;
    }

    /**
     * @return a Collection containing the identifiers of all entities in the listing whose timestamp
     *         was equal to {@link #getLatestTimestamp()}
     */
    @XmlTransient
    public Collection<String> getMatchingIdentifiers() {
        return matchingIdentifiers;
    }

    /**
     * Sets the Collection containing the identifiers of all entities in the listing whose Timestamp was
     * equal to {@link #getLatestTimestamp()}
     *
     * @param matchingIdentifiers the identifiers that have last modified date matching the latest timestamp
     */
    public void setMatchingIdentifiers(Collection<String> matchingIdentifiers) {
        this.matchingIdentifiers = matchingIdentifiers;
    }

}
