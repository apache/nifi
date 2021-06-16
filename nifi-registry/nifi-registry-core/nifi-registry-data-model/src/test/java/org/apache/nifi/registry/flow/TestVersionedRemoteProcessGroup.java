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
package org.apache.nifi.registry.flow;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestVersionedRemoteProcessGroup {

    @Test
    public void testGetTargetUriAndGetTargetUris() {

        VersionedRemoteProcessGroup vRPG = new VersionedRemoteProcessGroup();


        /* targetUri is null, targetUris varies */

        vRPG.setTargetUri(null);
        vRPG.setTargetUris(null);
        assertEquals(null, vRPG.getTargetUri());
        assertEquals(null, vRPG.getTargetUris());

        vRPG.setTargetUri(null);
        vRPG.setTargetUris("");
        assertEquals(null, vRPG.getTargetUri());
        assertEquals(null, vRPG.getTargetUris());

        vRPG.setTargetUri(null);
        vRPG.setTargetUris("uri-2");
        //assertEquals("uri-2", vRPG.getTargetUri());
        assertEquals("uri-2", vRPG.getTargetUris());

        vRPG.setTargetUri(null);
        vRPG.setTargetUris("uri-2,uri-3");
        assertEquals("uri-2", vRPG.getTargetUri());
        assertEquals("uri-2,uri-3", vRPG.getTargetUris());


        /* targetUri is empty, targetUris varies */

        vRPG.setTargetUri("");
        vRPG.setTargetUris(null);
        assertEquals(null, vRPG.getTargetUri());
        assertEquals(null, vRPG.getTargetUris());

        vRPG.setTargetUri("");
        vRPG.setTargetUris("");
        assertEquals(null, vRPG.getTargetUri());
        assertEquals(null, vRPG.getTargetUris());

        vRPG.setTargetUri("");
        vRPG.setTargetUris("uri-2");
        assertEquals("uri-2", vRPG.getTargetUri());
        assertEquals("uri-2", vRPG.getTargetUris());

        vRPG.setTargetUri("");
        vRPG.setTargetUris("uri-2,uri-3");
        assertEquals("uri-2", vRPG.getTargetUri());
        assertEquals("uri-2,uri-3", vRPG.getTargetUris());


        /* targetUri is set, targetUris varies */

        vRPG.setTargetUri("uri-1");
        vRPG.setTargetUris(null);
        assertEquals("uri-1", vRPG.getTargetUri());
        assertEquals("uri-1", vRPG.getTargetUris());

        vRPG.setTargetUri("uri-1");
        vRPG.setTargetUris("");
        assertEquals("uri-1", vRPG.getTargetUri());
        assertEquals("uri-1", vRPG.getTargetUris());

        vRPG.setTargetUri("uri-1");
        vRPG.setTargetUris("uri-2");
        assertEquals("uri-1", vRPG.getTargetUri());
        assertEquals("uri-2", vRPG.getTargetUris());

        vRPG.setTargetUri("uri-1");
        vRPG.setTargetUris("uri-2,uri-3");
        assertEquals("uri-1", vRPG.getTargetUri());
        assertEquals("uri-2,uri-3", vRPG.getTargetUris());

    }


}
