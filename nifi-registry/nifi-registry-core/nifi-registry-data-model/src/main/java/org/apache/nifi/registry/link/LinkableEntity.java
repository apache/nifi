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
package org.apache.nifi.registry.link;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.ws.rs.core.Link;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * Base classes for domain objects that want to provide a hypermedia link.
 */
@ApiModel
public abstract class LinkableEntity {

    private Link link;

    @XmlElement
    @XmlJavaTypeAdapter(LinkAdapter.class)
    @ApiModelProperty(value = "An WebLink to this entity.",
            dataType = "org.apache.nifi.registry.link.JaxbLink", readOnly = true)
    public Link getLink() {
        return link;
    }

    public void setLink(Link link) {
        this.link = link;
    }

}
