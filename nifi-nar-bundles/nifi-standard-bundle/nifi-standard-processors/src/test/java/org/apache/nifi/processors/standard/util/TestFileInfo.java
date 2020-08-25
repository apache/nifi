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

package org.apache.nifi.processors.standard.util;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TestFileInfo {

    @Test
    public void testPermissionModeToString() {
        String rwxPerm = FileInfo.permissionToString(0567);
        assertEquals("r-xrw-rwx", rwxPerm);

        // Test with sticky bit
        rwxPerm = FileInfo.permissionToString(01567);
        assertEquals("r-xrw-rwx", rwxPerm);

        rwxPerm = FileInfo.permissionToString(03);
        assertEquals("-------wx", rwxPerm);

    }

}
