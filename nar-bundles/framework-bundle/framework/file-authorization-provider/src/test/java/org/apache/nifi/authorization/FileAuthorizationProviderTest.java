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
package org.apache.nifi.authorization;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.file.FileUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.mockito.Mockito;

@Ignore
public class FileAuthorizationProviderTest {
    
    private FileAuthorizationProvider provider;
    
    private File primary;
    
    private File restore;
    
    private NiFiProperties mockProperties;
    
    private AuthorityProviderConfigurationContext mockConfigurationContext;
    
    @Before
    public void setup() throws IOException {
        
        primary = new File("target/primary/users.txt");
        restore = new File("target/restore/users.txt");
        
        System.out.println("absolute path: " + primary.getAbsolutePath());
        
        mockProperties = mock(NiFiProperties.class);
        when(mockProperties.getRestoreDirectory()).thenReturn(restore.getParentFile());
        
        mockConfigurationContext = mock(AuthorityProviderConfigurationContext.class);
        when(mockConfigurationContext.getProperty(Mockito.eq("Authorized Users File"))).thenReturn(primary.getPath());
        
        provider = new FileAuthorizationProvider();
        provider.setNiFiProperties(mockProperties);
        provider.initialize(null);
    }     
    
    @After
    public void cleanup() throws Exception {
        deleteFile(primary);
        deleteFile(restore);
    }
    
    private boolean deleteFile(final File file) {
        if(file.isDirectory()) {
            FileUtils.deleteFilesInDir(file, null, null, true, true);
        }
        return FileUtils.deleteFile(file, null, 10);
    }
    
    @Test
    public void testPostContructionWhenRestoreDoesNotExist() throws Exception {
        
        byte[] primaryBytes = "<users/>".getBytes();
        FileOutputStream fos = new FileOutputStream(primary);
        fos.write(primaryBytes);
        fos.close();
        
        provider.onConfigured(mockConfigurationContext);
        assertEquals(primary.length(), restore.length());
    }
    
    @Test
    public void testPostContructionWhenPrimaryDoesNotExist() throws Exception {
        
        byte[] restoreBytes = "<users/>".getBytes();
        FileOutputStream fos = new FileOutputStream(restore);
        fos.write(restoreBytes);
        fos.close();
        
        provider.onConfigured(mockConfigurationContext);
        assertEquals(restore.length(), primary.length());
        
    }
    
    @Test(expected = ProviderCreationException.class)
    public void testPostContructionWhenPrimaryDifferentThanRestore() throws Exception {
        
        byte[] primaryBytes = "<users></users>".getBytes();
        FileOutputStream fos = new FileOutputStream(primary);
        fos.write(primaryBytes);
        fos.close();
        
        byte[] restoreBytes = "<users/>".getBytes();
        fos = new FileOutputStream(restore);
        fos.write(restoreBytes);
        fos.close();
        
        provider.onConfigured(mockConfigurationContext);
    }
    
    @Test
    public void testPostContructionWhenPrimaryAndBackupDoNotExist() throws Exception {
        
        provider.onConfigured(mockConfigurationContext);
        assertEquals(0, restore.length());
        assertEquals(restore.length(), primary.length());
    }
    
}
