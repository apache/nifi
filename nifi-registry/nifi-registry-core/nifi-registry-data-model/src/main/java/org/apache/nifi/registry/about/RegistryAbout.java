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
package org.apache.nifi.registry.about;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement
@ApiModel
public class RegistryAbout {
    private final String registryAboutVersion;

    /**
     * Container for the version string of this Registry.
     *
     * @param registryAboutVersion the version string for this Registry
     */
    public RegistryAbout(String registryAboutVersion) {
        this.registryAboutVersion = registryAboutVersion;
    }

    /**
     * @return the version string of this NiFi Registry. This value is read only
     */
    @ApiModelProperty(
            value = "The version string for this Nifi Registry",
            readOnly = true
    )
    public String getRegistryAboutVersion() {
        return registryAboutVersion;
    }
}
