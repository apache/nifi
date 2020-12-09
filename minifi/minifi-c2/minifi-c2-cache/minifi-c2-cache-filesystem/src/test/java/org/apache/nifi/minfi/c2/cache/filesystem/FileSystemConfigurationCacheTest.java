/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.minfi.c2.cache.filesystem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;
import org.apache.nifi.minifi.c2.api.InvalidParameterException;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCacheFileInfo;
import org.apache.nifi.minifi.c2.api.cache.WriteableConfiguration;
import org.apache.nifi.minifi.c2.cache.filesystem.FileSystemConfigurationCache;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class FileSystemConfigurationCacheTest {

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Before
  public void before() {
    File resourcesDirectory = new File("src/test/resources/");
    environmentVariables.set("C2_SERVER_HOME", resourcesDirectory.getAbsolutePath());
  }

  @Test
  public void getConfigurationTest() throws IOException, ConfigurationProviderException {

    final String pathRoot = "files";
    final String pathPattern = "config";

    FileSystemConfigurationCache cache = new FileSystemConfigurationCache(pathRoot, pathPattern);

    Map<String, List<String>> parameters = new HashMap<>();

    ConfigurationCacheFileInfo info = cache.getCacheFileInfo("text/yaml", parameters);

    WriteableConfiguration configuration = info.getConfiguration(1);

    assertEquals("config.text.yaml.v1", configuration.getName());
    assertEquals("1", configuration.getVersion());
    assertTrue(configuration.exists());

  }

  @Test
  public void getNonexistantConfigurationTest() throws IOException, ConfigurationProviderException {

    final String pathRoot = "files";
    final String pathPattern = "config";

    FileSystemConfigurationCache cache = new FileSystemConfigurationCache(pathRoot, pathPattern);

    Map<String, List<String>> parameters = new HashMap<>();

    ConfigurationCacheFileInfo info = cache.getCacheFileInfo("test/contenttype", parameters);

    WriteableConfiguration configuration = info.getConfiguration(1);

    assertEquals("config.test.contenttype.v1", configuration.getName());
    assertEquals("1", configuration.getVersion());
    assertFalse(configuration.exists());

  }

  @Test
  public void getCachedConfigurationsTest() throws IOException, ConfigurationProviderException {

    final String pathRoot = "files";
    final String pathPattern = "config";

    FileSystemConfigurationCache cache = new FileSystemConfigurationCache(pathRoot, pathPattern);

    Map<String, List<String>> parameters = new HashMap<>();

    ConfigurationCacheFileInfo info = cache.getCacheFileInfo("text/yaml", parameters);

    Stream<WriteableConfiguration> configs = info.getCachedConfigurations();

    assertEquals(1, configs.count());

  }

  @Test(expected = InvalidParameterException.class)
  public void getConfigurationInvalidParametersTest() throws IOException,
      InvalidParameterException {

    final String pathRoot = "files";
    final String pathPattern = "${test}/config";

    FileSystemConfigurationCache cache = new FileSystemConfigurationCache(pathRoot, pathPattern);

    Map<String, List<String>> parameters = new HashMap<>();

    cache.getCacheFileInfo("test/contenttype", parameters);

  }

}
