/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.cluster.manager;

import org.apache.nifi.nar.NarState;
import org.apache.nifi.web.api.dto.NarCoordinateDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NarSummaryDtoMergerTest {

    public static final String NIFI_GROUP_ID = "org.apache.nifi";
    public static final String NIFI_NAR_ID = "nifi-nar-1";
    public static final String NIFI_NAR_VERSION = "1.0.0";

    @Test
    public void testMergeNarSummaryDtosInstallingIntoInstalled() {
        final String narId = UUID.randomUUID().toString();

        final NarSummaryDTO summary1 = new NarSummaryDTO();
        summary1.setIdentifier(narId);
        summary1.setCoordinate(new NarCoordinateDTO(NIFI_GROUP_ID, NIFI_NAR_ID, NIFI_NAR_VERSION));
        summary1.setState(NarState.INSTALLED.getValue());

        final NarSummaryDTO summary2 = new NarSummaryDTO();
        summary2.setIdentifier(narId);
        summary2.setCoordinate(new NarCoordinateDTO(NIFI_GROUP_ID, NIFI_NAR_ID, NIFI_NAR_VERSION));
        summary2.setState(NarState.INSTALLING.getValue());

        NarSummaryDtoMerger.merge(summary1, summary2);
        assertEquals(NarState.INSTALLING.getValue(), summary1.getState());
    }

    @Test
    public void testMergeNarSummaryDtosInstalledIntoInstalling() {
        final String narId = UUID.randomUUID().toString();

        final NarSummaryDTO summary1 = new NarSummaryDTO();
        summary1.setIdentifier(narId);
        summary1.setCoordinate(new NarCoordinateDTO(NIFI_GROUP_ID, NIFI_NAR_ID, NIFI_NAR_VERSION));
        summary1.setState(NarState.INSTALLING.getValue());

        final NarSummaryDTO summary2 = new NarSummaryDTO();
        summary2.setIdentifier(narId);
        summary2.setCoordinate(new NarCoordinateDTO(NIFI_GROUP_ID, NIFI_NAR_ID, NIFI_NAR_VERSION));
        summary2.setState(NarState.INSTALLED.getValue());

        NarSummaryDtoMerger.merge(summary1, summary2);
        assertEquals(NarState.INSTALLING.getValue(), summary1.getState());
    }
}
