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
package org.apache.nifi.monitor.thresholds.ui;
/*
 * NOTE: rule is synonymous with threshold
 */

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.nifi.settings.generated.Thresholds;
import org.apache.nifi.settings.generated.Attribute;
import org.apache.nifi.settings.generated.Configuration;
import org.apache.nifi.settings.generated.ObjectFactory;
import org.apache.nifi.web.HttpServletRequestContextConfig;
import org.apache.nifi.web.NiFiWebContext;

public class ThresholdsConfigFile {

    protected final Logger logger = LoggerFactory.getLogger(ThresholdsConfigFile.class);
    private final String THRESHOLD_SETTINGS_XSD = "/threshold_settings.xsd";
    private final String JAXB_GENERATED_PATH = "org.apache.nifi.settings.generated";
    private final JAXBContext JAXB_CONTEXT = initializeJaxbContext();
    private Configuration configuration;
    private ObjectFactory objectFactory;
    private final List<Attribute> filtered_attribute_list = new ArrayList<>();
    private final NiFiWebContext nifiWebCtx;
    private final HttpServletRequestContextConfig contextConfig;
    private final HttpServletRequest request;

    /**
     * Load the JAXBContext.
     */
    private JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH);
        } catch (JAXBException e) {
            logger.error("Unable to create JAXBContext.");
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    public ThresholdsConfigFile(final NiFiWebContext ctx, final HttpServletRequest request) {

        //logger.debug("Running ThresholdsConfigFile(...) constructor.");
        nifiWebCtx = ctx;
        this.request = request;
        contextConfig = new HttpServletRequestContextConfig(request);
        getState();
    }

    private void getState() {
        objectFactory = new ObjectFactory();
        String state = nifiWebCtx.getProcessor(contextConfig).getAnnotationData();
        if (state != null && !state.isEmpty()) {
            try {
                //logger.debug("Running ThresholdsConfigFile.getState(). Getting (and unmarshalling) Annotation Data."); 
                // find the schema
                SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                Schema schema = schemaFactory.newSchema(ThresholdsConfigFile.class.getResource(THRESHOLD_SETTINGS_XSD));

                // attempt to unmarshal
                Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
                unmarshaller.setSchema(schema);
                JAXBElement<Configuration> element = unmarshaller.unmarshal(new StreamSource(new StringReader(state)), Configuration.class);
                configuration = element.getValue();
            } catch (Exception ex) {
                logger.error(ex.getMessage());
            }
        } else {
            try {
                configuration = objectFactory.createConfiguration();
                save();
            } catch (Exception ex) {
                logger.error(ex.getMessage());
            }
        }
    }

    public List<Attribute> getFlowFileAttributes() {
        return configuration.getFlowFileAttribute();
    }

    /**
     * Helper method to determine if an attribute is already in the attributes
     * ArrayList.
     *
     * @param id
     * @return
     */
    public boolean containsAttribute(String id) {
        boolean result = false;
        for (Attribute attr : configuration.getFlowFileAttribute()) {
            if (attr.getId().compareTo(id) == 0) {
                result = true;
                break;
            }
        }
        return result;
    }

    public boolean containsRule(String id, String value) {
        boolean result = false;
        Attribute attr = findAttribute(id);
        if (attr != null) {
            Thresholds rule = findRule(attr, value);
            if (rule != null) {
                result = true;
            }

        }
        return result;
    }

    /**
     * Iterates through attributes list and returns the attribute that contains
     * the id passed into the method. Returns null if attribute is not found in
     * attribute list.
     * @param id
     * @return 
     */
    public Attribute findAttribute(String id) {

        for (Attribute attr : configuration.getFlowFileAttribute()) {
            if (attr.getId().compareTo(id) == 0) {
                return attr;
            }
        }
        return null;
    }

    public Attribute findAttributebyAttributeName(String attributeName) {

        for (Attribute attr : configuration.getFlowFileAttribute()) {
            if (attr.getAttributeName().compareTo(attributeName) == 0) {
                return attr;
            }
        }
        return null;
    }

    public Thresholds findRule(Attribute attr, String value) {
        for (Thresholds rule : attr.getRule()) {
            if (rule.getId().compareTo(value) == 0) {
                return rule;
            }
        }

        return null;
    }

    /**
     * returns xml without attributes, used for presentation.
     *
     * @param col
     * @param isAsc
     * @param attributeFilter
     * @param rowNum
     * @param pageCom
     * @return
     */
    public String getConfigAttributes(String col, Boolean isAsc, String attributeFilter, Integer rowNum, String pageCom) {
        return getFormattedAttributes(configuration.getFlowFileAttribute(), col, isAsc, attributeFilter, "", "", "");//,rowNum,pageCom);
    }

    /**
     * UI Specific. Returns xml only related to the attributeid passed into the
     * method.
     * @param attrid
     * @param col
     * @param isAsc
     * @param attributevalueFilter
     * @param sizeFilter
     * @param filecountfilter
     * @return 
     */
    public String getRules(String attrid, String col, Boolean isAsc, String attributevalueFilter, String sizeFilter, String filecountfilter) {
        List<Attribute> attributeList = new ArrayList<>();
        Attribute attr = findAttribute(attrid);
        if (attr != null) {
            attributeList.add(attr);
        }

        Collections.sort(attributeList.get(0).getRule(), new RuleComparator(col));

        if (!isAsc) {
            Collections.reverse(attributeList.get(0).getRule());
        }

        return getFormattedAttributes(attributeList, col, isAsc, "", attributevalueFilter, sizeFilter, filecountfilter);//,-1,"");
    }

    public String getListStats()//Integer rowNum)
    {
        return String.format("Results %s", filtered_attribute_list.size());
    }

    private String getFormattedAttributes(List<Attribute> list, String col, Boolean isAsc, String attributeFilter, String attributevalueFilter,
            String sizeFilter, String filecountfilter)//, Integer rowNum, String pageCom)
    {
        String result = "<?xml version=\"1.0\" ?><configuration><attributes>";

        getFilteredList(list, attributeFilter);

        for (Attribute attribute : getSortedPagedList(col, isAsc)) {
            result += "<attribute>";
            result += String.format("<uuid>%s</uuid>", attribute.getId());
            result += String.format("<attributename>%s</attributename>", attribute.getAttributeName());
            result += "<rules>";
            result += "<rule>";
            result += String.format("<ruuid>%s</ruuid>", UUID.randomUUID());
            result += String.format("<attributevalue>Default</attributevalue>");
            result += String.format("<size>%s</size>", attribute.getNoMatchRule().getSize());
            result += String.format("<count>%s</count>", attribute.getNoMatchRule().getCount());
            result += "</rule>";
            for (Thresholds rule : attribute.getRule()) {
                if (rule.getId().toLowerCase().contains(attributevalueFilter.toLowerCase())
                        && rule.getSize().toString().contains(sizeFilter)
                        && rule.getCount().toString().contains(filecountfilter)) {
                    result += "<rule>";
                    result += String.format("<ruuid>%s</ruuid>", UUID.randomUUID());
                    result += String.format("<attributevalue>%s</attributevalue>", rule.getId());
                    result += String.format("<size>%s</size>", rule.getSize());
                    result += String.format("<count>%s</count>", rule.getCount());
                    result += "</rule>";
                }
            }
            result += "</rules></attribute>";
        }

        result += "</attributes></configuration>";
        return result;

    }

    private void sortList(List<Attribute> list, String col, boolean isAsc) {
        Collections.sort(list, new AttributeComparator(col));

        if (!isAsc) {
            Collections.reverse(list);
        }
    }

    private void getFilteredList(List<Attribute> list, String attributeFilter) {
        filtered_attribute_list.clear();
        for (Attribute attribute : list) {
            if (attribute.getAttributeName().toLowerCase().contains(attributeFilter.toLowerCase())) {
                filtered_attribute_list.add(attribute);
            }
        }
    }

    private List<Attribute> getSortedPagedList(String col, boolean isAsc) {
        sortList(filtered_attribute_list, col, isAsc);//sub, col, isAsc);
        return filtered_attribute_list;// sub;
    }

    public void save() throws Exception {
        final StringWriter strWriter = new StringWriter();
        Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        marshaller.marshal(configuration, strWriter);
        // save thresholds (in the framework/flow.xml) for MonitorThreshold processor to read and use. 
        nifiWebCtx.setProcessorAnnotationData(contextConfig, strWriter.toString());
    }
}
