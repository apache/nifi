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
package org.apache.nifi.web.api.dto;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestRemoteProcessGroupDTO {

    @Test
    public void testGetTargetUriAndUris() {
        final RemoteProcessGroupDTO dto = new RemoteProcessGroupDTO();

        assertNull(dto.getTargetUri());

        dto.setTargetUris("http://node1:8080/nifi, http://node2:8080/nifi");
        assertEquals("If targetUris are set but targetUri is not, it should returns the first uru of the targetUris",
                "http://node1:8080/nifi", dto.getTargetUri());
        assertEquals("http://node1:8080/nifi, http://node2:8080/nifi", dto.getTargetUris());

        dto.setTargetUri("http://node3:9090/nifi");
        assertEquals("If both targetUri and targetUris are set, each returns its own values",
                "http://node3:9090/nifi", dto.getTargetUri());
        assertEquals("http://node1:8080/nifi, http://node2:8080/nifi", dto.getTargetUris());

        dto.setTargetUris(null);
        assertEquals("http://node3:9090/nifi", dto.getTargetUri());
        assertEquals("getTargetUris should return targetUri when it's not set",
                "http://node3:9090/nifi", dto.getTargetUris());

    }

}
