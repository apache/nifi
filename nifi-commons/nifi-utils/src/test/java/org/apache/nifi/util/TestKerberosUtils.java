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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;


@RunWith(PowerMockRunner.class)
@PrepareForTest({KerberosUtils.class, System.class, InetAddress.class})
public class TestKerberosUtils {


    @Test
    public void testReplaceHostname() throws UnknownHostException {
        PowerMockito.mockStatic(InetAddress.class);
        InetAddress inet = Mockito.mock(InetAddress.class);

        PowerMockito.when(InetAddress.getLocalHost()).thenReturn(inet);
        PowerMockito.when(inet.getCanonicalHostName()).thenReturn("my_hostname");
        assertEquals("service/my_hostname@realm", KerberosUtils.replaceHostname("service/_HOST@realm"));
    }

    @Test
    public void testNotReplaceHostname() {
        assertEquals("service/_HOS@realm", KerberosUtils.replaceHostname("service/_HOS@realm"));
    }

    @Test
    public void testInvalidHostname() throws UnknownHostException {
        PowerMockito.mockStatic(InetAddress.class);
        InetAddress inet = Mockito.mock(InetAddress.class);

        PowerMockito.when(InetAddress.getLocalHost()).thenReturn(inet);
        PowerMockito.when(inet.getCanonicalHostName()).thenThrow(UnknownHostException.class);
        assertEquals("service/_HOST@realm", KerberosUtils.replaceHostname("service/_HOST@realm"));
    }

}
