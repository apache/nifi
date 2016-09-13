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

package org.apache.nifi.remote;

import static org.junit.Assert.assertEquals;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Test;
import org.mockito.Mockito;

public class TestStandardRemoteProcessGroup {

    @Test
    public void testApiUri() {
        final NiFiProperties properties = Mockito.mock(NiFiProperties.class);
        final FlowController controller = Mockito.mock(FlowController.class);
        final ProcessGroup group = Mockito.mock(ProcessGroup.class);

        final String expectedUri = "http://localhost:8080/nifi-api";
        StandardRemoteProcessGroup rpg = new StandardRemoteProcessGroup("id", "http://localhost:8080/nifi", group, controller, null, properties);
        assertEquals(expectedUri, rpg.getApiUri());

        rpg = new StandardRemoteProcessGroup("id", "http://localhost:8080/nifi/", group, controller, null, properties);
        assertEquals(expectedUri, rpg.getApiUri());

        rpg = new StandardRemoteProcessGroup("id", "http://localhost:8080/nifi/ ", group, controller, null, properties);
        assertEquals(expectedUri, rpg.getApiUri());

        rpg = new StandardRemoteProcessGroup("id", " http://localhost:8080/nifi/ ", group, controller, null, properties);
        assertEquals(expectedUri, rpg.getApiUri());

        rpg = new StandardRemoteProcessGroup("id", "http://localhost:8080/", group, controller, null, properties);
        assertEquals(expectedUri, rpg.getApiUri());

        rpg = new StandardRemoteProcessGroup("id", "http://localhost:8080", group, controller, null, properties);
        assertEquals(expectedUri, rpg.getApiUri());

        rpg = new StandardRemoteProcessGroup("id", "http://localhost:8080 ", group, controller, null, properties);
        assertEquals(expectedUri, rpg.getApiUri());
    }

}
