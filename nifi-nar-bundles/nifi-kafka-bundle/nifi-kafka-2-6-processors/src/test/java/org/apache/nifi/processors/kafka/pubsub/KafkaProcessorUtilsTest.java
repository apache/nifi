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
package org.apache.nifi.processors.kafka.pubsub;

import org.apache.nifi.kerberos.SelfContainedKerberosUserService;
import org.apache.nifi.security.krb.KerberosUser;
import org.junit.jupiter.api.Test;

import javax.security.auth.login.AppConfigurationEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaProcessorUtilsTest {

  @Test
  public void getTransactionalIdSupplierWithPrefix() {
    Supplier<String> prefix = KafkaProcessorUtils.getTransactionalIdSupplier("prefix");
    String id = prefix.get();
    assertTrue(id.startsWith("prefix"));
    assertEquals(42, id.length());
  }

  @Test
  public void getTransactionalIdSupplierWithEmptyPrefix() {
    Supplier<String> prefix = KafkaProcessorUtils.getTransactionalIdSupplier(null);
    assertEquals(36, prefix.get().length() );
  }

  @Test
  public void testCreateJaasConfigFromKerberosUser() {
    final String loginModule = "com.sun.security.auth.module.Krb5LoginModule";
    final AppConfigurationEntry.LoginModuleControlFlag controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;

    final Map<String,String> options = new HashMap<>();
    options.put("option1", "value1");
    options.put("option2", "value2");

    final AppConfigurationEntry configEntry = new AppConfigurationEntry(loginModule, controlFlag, options);

    final KerberosUser kerberosUser = mock(KerberosUser.class);
    when(kerberosUser.getConfigurationEntry()).thenReturn(configEntry);

    final SelfContainedKerberosUserService kerberosUserService = mock(SelfContainedKerberosUserService.class);
    when(kerberosUserService.createKerberosUser()).thenReturn(kerberosUser);

    final String jaasConfig = KafkaProcessorUtils.createGssApiJaasConfig(kerberosUserService);
    assertNotNull(jaasConfig);
    assertEquals("com.sun.security.auth.module.Krb5LoginModule required option1=\"value1\" option2=\"value2\";", jaasConfig);
  }
}