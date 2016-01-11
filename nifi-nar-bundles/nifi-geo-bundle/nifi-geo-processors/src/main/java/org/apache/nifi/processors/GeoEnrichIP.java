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
package org.apache.nifi.processors;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.maxmind.DatabaseReader;
import org.apache.nifi.util.StopWatch;

import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Subdivision;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"geo", "enrich", "ip", "maxmind"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Looks up geolocation information for an IP address and adds the geo information to FlowFile attributes. The "
        + "geo data is provided as a MaxMind database. The attribute that contains the IP address to lookup is provided by the "
        + "'IP Address Attribute' property. If the name of the attribute provided is 'X', then the the attributes added by enrichment "
        + "will take the form X.geo.<fieldName>")
@WritesAttributes({
    @WritesAttribute(attribute = "X.geo.lookup.micros", description = "The number of microseconds that the geo lookup took"),
    @WritesAttribute(attribute = "X.geo.city", description = "The city identified for the IP address"),
    @WritesAttribute(attribute = "X.geo.latitude", description = "The latitude identified for this IP address"),
    @WritesAttribute(attribute = "X.geo.longitude", description = "The longitude identified for this IP address"),
    @WritesAttribute(attribute = "X.geo.subdivision.N",
            description = "Each subdivision that is identified for this IP address is added with a one-up number appended to the attribute name, starting with 0"),
    @WritesAttribute(attribute = "X.geo.subdivision.isocode.N", description = "The ISO code for the subdivision that is identified by X.geo.subdivision.N"),
    @WritesAttribute(attribute = "X.geo.country", description = "The country identified for this IP address"),
    @WritesAttribute(attribute = "X.geo.country.isocode", description = "The ISO Code for the country identified"),
    @WritesAttribute(attribute = "X.geo.postalcode", description = "The postal code for the country identified"),})
public class GeoEnrichIP extends AbstractProcessor {

    public static final PropertyDescriptor GEO_DATABASE_FILE = new PropertyDescriptor.Builder()
            .name("Geo Database File")
            .description("Path to Maxmind Geo Enrichment Database File")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor IP_ADDRESS_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("IP Address Attribute")
            .required(true)
            .description("The name of an attribute whose value is a dotted decimal IP address for which enrichment should occur")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_FOUND = new Relationship.Builder()
            .name("found")
            .description("Where to route flow files after successfully enriching attributes with geo data")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("Where to route flow files after unsuccessfully enriching attributes because no geo data was found")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> propertyDescriptors;
    private final AtomicReference<DatabaseReader> databaseReaderRef = new AtomicReference<>(null);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public final void onScheduled(final ProcessContext context) throws IOException {
        final String dbFileString = context.getProperty(GEO_DATABASE_FILE).getValue();
        final File dbFile = new File(dbFileString);
        final StopWatch stopWatch = new StopWatch(true);
        final DatabaseReader reader = new DatabaseReader.Builder(dbFile).build();
        stopWatch.stop();
        getLogger().info("Completed loading of Maxmind Geo Database.  Elapsed time was {} milliseconds.", new Object[]{stopWatch.getDuration(TimeUnit.MILLISECONDS)});
        databaseReaderRef.set(reader);
    }

    @OnStopped
    public void closeReader() throws IOException {
        final DatabaseReader reader = databaseReaderRef.get();
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_FOUND);
        rels.add(REL_NOT_FOUND);
        this.relationships = Collections.unmodifiableSet(rels);

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(GEO_DATABASE_FILE);
        props.add(IP_ADDRESS_ATTRIBUTE);
        this.propertyDescriptors = Collections.unmodifiableList(props);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final DatabaseReader dbReader = databaseReaderRef.get();
        final String ipAttributeName = context.getProperty(IP_ADDRESS_ATTRIBUTE).getValue();
        final String ipAttributeValue = flowFile.getAttribute(ipAttributeName);
        if (StringUtils.isEmpty(ipAttributeName)) { //TODO need to add additional validation - should look like an IPv4 or IPv6 addr for instance
            session.transfer(flowFile, REL_NOT_FOUND);
            getLogger().warn("Unable to find ip address for {}", new Object[]{flowFile});
            return;
        }
        InetAddress inetAddress = null;
        CityResponse response = null;

        try {
            inetAddress = InetAddress.getByName(ipAttributeValue);
        } catch (final IOException ioe) {
            session.transfer(flowFile, REL_NOT_FOUND);
            getLogger().warn("Could not resolve {} to ip address for {}", new Object[]{ipAttributeValue, flowFile}, ioe);
            return;
        }
        final StopWatch stopWatch = new StopWatch(true);
        try {
            response = dbReader.city(inetAddress);
            stopWatch.stop();
        } catch (final IOException | GeoIp2Exception ex) {
            session.transfer(flowFile, REL_NOT_FOUND);
            getLogger().warn("Failure while trying to find enrichment data for {} due to {}", new Object[]{flowFile, ex}, ex);
            return;
        }

        if (response == null) {
            session.transfer(flowFile, REL_NOT_FOUND);
            return;
        }

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(new StringBuilder(ipAttributeName).append(".geo.lookup.micros").toString(), String.valueOf(stopWatch.getDuration(TimeUnit.MICROSECONDS)));
        attrs.put(new StringBuilder(ipAttributeName).append(".geo.city").toString(), response.getCity().getName());

        final Double latitude = response.getLocation().getLatitude();
        if (latitude != null) {
            attrs.put(new StringBuilder(ipAttributeName).append(".geo.latitude").toString(), latitude.toString());
        }

        final Double longitude = response.getLocation().getLongitude();
        if (longitude != null) {
            attrs.put(new StringBuilder(ipAttributeName).append(".geo.longitude").toString(), longitude.toString());
        }

        int i = 0;
        for (final Subdivision subd : response.getSubdivisions()) {
            attrs.put(new StringBuilder(ipAttributeName).append(".geo.subdivision.").append(i).toString(), subd.getName());
            attrs.put(new StringBuilder(ipAttributeName).append(".geo.subdivision.isocode.").append(i).toString(), subd.getIsoCode());
            i++;
        }
        attrs.put(new StringBuilder(ipAttributeName).append(".geo.country").toString(), response.getCountry().getName());
        attrs.put(new StringBuilder(ipAttributeName).append(".geo.country.isocode").toString(), response.getCountry().getIsoCode());
        attrs.put(new StringBuilder(ipAttributeName).append(".geo.postalcode").toString(), response.getPostal().getCode());
        flowFile = session.putAllAttributes(flowFile, attrs);

        session.transfer(flowFile, REL_FOUND);
    }

}
