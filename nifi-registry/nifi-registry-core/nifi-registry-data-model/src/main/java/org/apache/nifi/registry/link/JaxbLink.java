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

import javax.xml.bind.annotation.XmlAnyAttribute;
import javax.xml.bind.annotation.XmlAttribute;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Copy of JAX-RS Link.JaxbLink so that Swagger annotations can be applied properly so that getUri() lines up with "href".
 */
@ApiModel
public class JaxbLink {

    private URI uri;
    private Map<String,String> params;

    /**
     * Default constructor needed during unmarshalling.
     */
    public JaxbLink() {
    }

    /**
     * Construct an instance from a URI and no parameters.
     *
     * @param uri underlying URI.
     */
    public JaxbLink(URI uri) {
        this.uri = uri;
    }

    /**
     * Construct an instance from a URI and some parameters.
     *
     * @param uri    underlying URI.
     * @param params parameters of this link.
     */
    public JaxbLink(URI uri, Map<String,String> params) {
        this.uri = uri;
        this.params = params;
    }

    /**
     * Get the underlying URI for this link.
     *
     * @return underlying URI.
     */
    @XmlAttribute(name = "href")
    @ApiModelProperty(name = "href", value = "The href for the link")
    public URI getUri() {
        return uri;
    }

    /**
     * Get the parameter map for this link.
     *
     * @return parameter map.
     */
    @XmlAnyAttribute
    @ApiModelProperty(name = "params", value = "The params for the link")
    public Map<String,String> getParams() {
        if (params == null) {
            params = new HashMap<>();
        }
        return params;
    }

    /**
     * Set the underlying URI for this link.
     *
     * This setter is needed for JAXB unmarshalling.
     */
    void setUri(URI uri) {
        this.uri = uri;
    }

    /**
     * Set the parameter map for this link.
     *
     * This setter is needed for JAXB unmarshalling.
     */
    void setParams(Map<String,String> params) {
        this.params = params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JaxbLink)) return false;

        JaxbLink jaxbLink = (JaxbLink) o;

        if (uri != null ? !uri.equals(jaxbLink.uri) : jaxbLink.uri != null) {
            return false;
        }

        if (params == jaxbLink.params) {
            return true;
        }
        if (params == null) {
            // if this.params is 'null', consider other.params equal to empty
            return jaxbLink.params.isEmpty();
        }
        if (jaxbLink.params == null) {
            // if other.params is 'null', consider this.params equal to empty
            return params.isEmpty();
        }

        return params.equals(jaxbLink.params);
    }

    @Override
    public int hashCode() {
        int result = uri != null ? uri.hashCode() : 0;
        result = 31 * result + (params != null && !params.isEmpty() ? params.hashCode() : 0);
        return result;
    }

}
