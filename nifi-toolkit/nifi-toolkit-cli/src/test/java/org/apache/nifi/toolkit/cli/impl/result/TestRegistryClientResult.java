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
package org.apache.nifi.toolkit.cli.impl.result;

import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.web.api.dto.RegistryDTO;
import org.apache.nifi.web.api.entity.RegistryClientEntity;
import org.apache.nifi.web.api.entity.RegistryClientsEntity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class TestRegistryClientResult {

    private ByteArrayOutputStream outputStream;
    private PrintStream printStream;

    @Before
    public void setup() {
        this.outputStream = new ByteArrayOutputStream();
        this.printStream = new PrintStream(outputStream, true);
    }

    @Test
    public void testWriteSimpleRegistryClientsResult() throws IOException {
        final RegistryDTO r1 = new RegistryDTO();
        r1.setName("Registry 1");
        r1.setUri("http://thisisalonglonglonglonglonglonglonglonglonguri.com:18080");
        r1.setId(UUID.fromString("ea752054-22c6-4fc0-b851-967d9a3837cb").toString());

        final RegistryDTO r2 = new RegistryDTO();
        r2.setName("Registry 2 with a longer than usual name");
        r2.setUri("http://localhost:18080");
        r2.setId(UUID.fromString("ddf5f289-7502-46df-9798-4b0457c1816b").toString());

        final RegistryClientEntity clientEntity1 = new RegistryClientEntity();
        clientEntity1.setId(r1.getId());
        clientEntity1.setComponent(r1);

        final RegistryClientEntity clientEntity2 = new RegistryClientEntity();
        clientEntity2.setId(r2.getId());
        clientEntity2.setComponent(r2);

        final Set<RegistryClientEntity> clientEntities = new HashSet<>();
        clientEntities.add(clientEntity1);
        clientEntities.add(clientEntity2);

        final RegistryClientsEntity registryClientsEntity = new RegistryClientsEntity();
        registryClientsEntity.setRegistries(clientEntities);

        final RegistryClientsResult result = new RegistryClientsResult(ResultType.SIMPLE, registryClientsEntity);
        result.write(printStream);

        final String resultOut = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        //System.out.println(resultOut);

        final String expected = "\n" +
                "#   Name                                   Id                                     Uri                                                               \n" +
                "-   ------------------------------------   ------------------------------------   ---------------------------------------------------------------   \n" +
                "1   Registry 1                             ea752054-22c6-4fc0-b851-967d9a3837cb   http://thisisalonglonglonglonglonglonglonglonglonguri.com:18080   \n" +
                "2   Registry 2 with a longer than usu...   ddf5f289-7502-46df-9798-4b0457c1816b   http://localhost:18080                                            \n" +
                "\n";

        Assert.assertEquals(expected, resultOut);
    }

}
