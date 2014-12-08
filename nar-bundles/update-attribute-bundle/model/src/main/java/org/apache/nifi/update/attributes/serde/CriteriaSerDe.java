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
package org.apache.nifi.update.attributes.serde;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import org.apache.nifi.update.attributes.Criteria;
import org.apache.nifi.update.attributes.FlowFilePolicy;
import org.apache.nifi.update.attributes.Rule;

/**
 *
 */
public class CriteriaSerDe {

    /**
     * Handles the Criteria binding during the (de)serialization process. This
     * is done because the Criteria itself, maintains a slightly different
     * model. The conversion between the models was being performed in the
     * getter/setter of the rules. Different implementations of JAXB functioned
     * differently (calling the setter vs calling the getter and adding to it)
     * and at times would not work. Another approach would have been to convert
     * between the models using a JAXB adapter. Unfortunately, this would have
     * required updates to all of the clients of Criteria. Keeping the binding
     * simple and initializing the Criteria with it seemed most appropriate.
     */
    @XmlRootElement(name = "criteria")
    private static class CriteriaBinding {

        private List<Rule> rules = null;
        private FlowFilePolicy flowFilePolicy = FlowFilePolicy.USE_CLONE;

        public List<Rule> getRules() {
            return rules;
        }

        public void setRules(List<Rule> rules) {
            this.rules = rules;
        }

        public FlowFilePolicy getFlowFilePolicy() {
            return flowFilePolicy;
        }

        public void setFlowFilePolicy(FlowFilePolicy flowFilePolicy) {
            this.flowFilePolicy = flowFilePolicy;
        }
    }

    /**
     * Serializes the specified criteria.
     *
     * @param criteria
     * @return
     */
    public static String serialize(final Criteria criteria) {
        final StringWriter writer = new StringWriter();

        try {
            // create the binding for the criteria
            final CriteriaBinding binding = new CriteriaBinding();
            binding.setFlowFilePolicy(criteria.getFlowFilePolicy());
            binding.setRules(criteria.getRules());

            // serialize the binding
            final JAXBContext context = JAXBContext.newInstance(CriteriaBinding.class);
            final Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            marshaller.marshal(binding, writer);
        } catch (final JAXBException jaxbe) {
            throw new IllegalArgumentException(jaxbe);
        }

        return writer.toString();
    }

    /**
     * Deserializes the specified criteria.
     *
     * @param string
     * @return
     */
    public static Criteria deserialize(final String string) {
        Criteria criteria = null;

        if (string != null && !string.trim().equals("")) {
            try {
                // deserialize the binding
                final JAXBContext context = JAXBContext.newInstance(CriteriaBinding.class);
                final Unmarshaller unmarshaller = context.createUnmarshaller();
                final Source source = new StreamSource(new StringReader(string));
                final JAXBElement<CriteriaBinding> element = unmarshaller.unmarshal(source, CriteriaBinding.class);

                // create the criteria from the binding
                final CriteriaBinding binding = element.getValue();
                criteria = new Criteria(binding.getFlowFilePolicy(), binding.getRules());
            } catch (final JAXBException jaxbe) {
                throw new IllegalArgumentException(jaxbe);
            }
        }

        return criteria;
    }
}
