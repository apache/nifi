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

package org.apache.nifi.processors.enrich;

import org.mockito.Mockito;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.spi.InitialContextFactory;
import java.util.Hashtable;


public class FakeDNSInitialDirContextFactory implements InitialContextFactory {
    private static DirContext mockContext = null;

    public static DirContext getLatestMockContext() {
        if (mockContext == null) {
            return CreateMockContext();
        } else {
            return mockContext;
        }
    }

    @Override
    public Context getInitialContext(Hashtable environment) throws NamingException {
        return CreateMockContext();
    }

    private static DirContext CreateMockContext() {
        synchronized (FakeDNSInitialDirContextFactory.class) {
            mockContext = (DirContext) Mockito.mock(DirContext.class);
        }
        return mockContext;
    }
}

