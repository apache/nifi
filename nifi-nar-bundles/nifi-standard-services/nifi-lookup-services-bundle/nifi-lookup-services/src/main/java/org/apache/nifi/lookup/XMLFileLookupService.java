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
package org.apache.nifi.lookup;

import org.apache.commons.configuration2.XMLConfiguration;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.lookup.configuration2.CommonsConfigurationLookupService;

@Tags({"lookup", "cache", "enrich", "join", "xml", "reloadable", "key", "value"})
@CapabilityDescription("A reloadable XML file-based lookup service." +
        " This service uses Apache Commons Configuration." +
        " Example XML configuration file and how to access specific configuration can be found at" +
        " http://commons.apache.org/proper/commons-configuration/userguide/howto_hierarchical.html")
public class XMLFileLookupService extends CommonsConfigurationLookupService<XMLConfiguration> {

}
