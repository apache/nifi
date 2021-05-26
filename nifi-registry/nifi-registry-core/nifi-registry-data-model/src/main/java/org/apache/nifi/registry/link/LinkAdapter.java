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

import javax.ws.rs.core.Link;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.util.Map;

/**
 * This class is a modified version of Jersey's Link.JaxbAdapter that adds protection against nulls.
 */
public class LinkAdapter extends XmlAdapter<JaxbLink, Link> {

    /**
     * Convert a {@link JaxbLink} into a {@link Link}.
     *
     * @param v instance of type {@link JaxbLink}.
     * @return mapped instance of type {@link Link.JaxbLink}
     */
    @Override
    public Link unmarshal(JaxbLink v) {
        if (v == null) {
            return null;
        }

        Link.Builder lb = Link.fromUri(v.getUri());
        if (v.getParams() != null) {
            for (Map.Entry<String,String> e : v.getParams().entrySet()) {
                lb.param(e.getKey(), e.getValue());
            }
        }
        return lb.build();
    }

    /**
     * Convert a {@link Link} into a {@link JaxbLink}.
     *
     * @param v instance of type {@link Link}.
     * @return mapped instance of type {@link JaxbLink}.
     */
    @Override
    public JaxbLink marshal(Link v) {
        if (v == null) {
           return null;
        }

        final JaxbLink jl = new JaxbLink(v.getUri());
        if (v.getParams() != null) {
            for (Map.Entry<String, String> e : v.getParams().entrySet()) {
                jl.getParams().put(e.getKey(), e.getValue());
            }
        }
        return jl;
    }
}
