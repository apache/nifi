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
package org.apache.nifi.processors.jmx;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.ReflectionException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Tags({"JMX"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@SeeAlso({})
@CapabilityDescription(
        "Connects to the JMX RMI Url on the configured hostname and port.  "
                + "All domains are queried and can be filtered by providing the full domain name "
                + "and optional MBean type as whitelist or blacklist parameters.\n\n"
                + "Blacklist example to exclude all types that start with 'Memory' and GarbageCollector from the "
                + "java.lang domain and everything from the java.util.logging domain:"
                + "\n\njava.lang:Memory.* GarbageCollector,java.util.logging")
@WritesAttributes({
        @WritesAttribute(attribute="hostname", description="The name of the host that the object originates."),
        @WritesAttribute(attribute="port", description="The JMX connection port that the object originates."),
        @WritesAttribute(attribute="timestamp", description="The timestamp of when the object was emitted.") })

public class GetJMX extends AbstractProcessor {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor
            .Builder().name("Hostname")
            .displayName("Hostname")
            .description("The JMX Hostname or IP address")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Port")
            .displayName("Port")
            .description("The JMX Port")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor WHITELIST = new PropertyDescriptor
            .Builder().name("DomainWhiteList")
            .displayName("Domain White List")
            .description("Include only these MBean domain(s) and type(s).  Domains are comma delimited "
                    + "and optional MBean types follow a colon and are space delimited.   "
                    + "Format: [domain1[:type1[ type2]][,domain2]]")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor BLACKLIST = new PropertyDescriptor
            .Builder().name("DomainBlackList")
            .displayName("Domain Black List")
            .description("Include everything excluding these MBean domain(s) and type(s).   Domains are "
                    + "comma delimited and optional MBean types follow a colon and are space delimited.   "
                    + "Format: [domain1[:type1[ type2]][,domain2]])")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor POLLING_INTERVAL = new PropertyDescriptor
            .Builder().name("PollingInterval")
            .displayName("Polling Interval")
            .description("Indicates how long to wait before performing a connection to the RMI Server")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("300 sec")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
            .Builder().name("BatchSize")
            .displayName("Batch Size")
            .description("The maximum number of MBean records to pull in each iteration")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are created are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that encounter errors are routed to this relationship")
            .build();

    public static final String HOSTNAME_ATTRIBUTE = "hostname";
    public static final String PORT_ATTRIBUTE = "port";
    public static final String TIMESTAMP_ATTRIBUTE = "timestamp";

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    private final BlockingQueue<Record> queue = new LinkedBlockingQueue<>();

    private final Set<Record> inProcess = new HashSet<>();    // guarded by queueLock
    private final Set<Record> recentlyProcessed = new HashSet<>();    // guarded by queueLock

    private final Lock queueLock = new ReentrantLock();
    private final Lock listingLock = new ReentrantLock();
    private final AtomicLong queueLastUpdated = new AtomicLong(0L);

    private final JsonBuilderFactory jsonBuilderFactory = Json.createBuilderFactory(null);

    private ListFilter whiteListFilter;
    private ListFilter blackListFilter;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(HOSTNAME);
        descriptors.add(PORT);
        descriptors.add(WHITELIST);
        descriptors.add(BLACKLIST);
        descriptors.add(POLLING_INTERVAL);
        descriptors.add(BATCH_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
        whiteListFilter = new ListFilter( "" );
        blackListFilter = new ListFilter( "" );
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        queue.clear();
    }

    private Set<Record> performQuery( ProcessContext context) throws ProcessException {
        PropertyValue hostname = context.getProperty(HOSTNAME);
        PropertyValue port = context.getProperty(PORT);
        PropertyValue whitelist = context.getProperty(WHITELIST);
        PropertyValue blacklist = context.getProperty(BLACKLIST);

        Set<Record> metricsSet = new HashSet<Record>();

        whiteListFilter.setListString(whitelist.getValue());
        blackListFilter.setListString( blacklist.getValue() );

        try {
            HashMap<String,Object> env = new HashMap<String,Object>();

            StringBuilder urlStr = new StringBuilder("service:jmx:rmi:///jndi/rmi://");
            urlStr.append(hostname).append(":");
            urlStr.append(port);
            urlStr.append("/jmxrmi");

            JMXServiceURL url = new JMXServiceURL(urlStr.toString());
            JMXConnector jmxc = JMXConnectorFactory.connect(url, null);

            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            Set<ObjectName> mbeans = mbsc.queryNames(null, null);
            for (ObjectName mbean : mbeans) {
                String domain = mbean.getDomain();

                Record record = new Record(jsonBuilderFactory);
                Hashtable<String, String> mbeanProperties = mbean.getKeyPropertyList();

                String type = (mbeanProperties.containsKey("type")) ? mbeanProperties.get("type") : "";

                if (canProcess(domain, type)) {
                    record.setDomain( domain );
                    record.setProperties(mbeanProperties.entrySet());

                    MBeanInfo mbeanInfo = mbsc.getMBeanInfo(mbean);
                    MBeanAttributeInfo[] mbeanAttributeInfoAr = mbeanInfo.getAttributes();

                    boolean validRecord = true;

                    Map<String, String> mbeanStringMap = new HashMap<String, String>();

                    for (MBeanAttributeInfo mbeanAttributeInfo : mbeanAttributeInfoAr) {
                        try {
                            Object value = mbsc.getAttribute(mbean, mbeanAttributeInfo.getName());

                            mbeanStringMap.put( mbeanAttributeInfo.getName(), value.toString() );
                        } catch (Exception e) {
                            // IF ERROR DO NOT ADD to metricsSet
                            validRecord = false;
                            getLogger().warn("Exception fetching MBean attribute: " + e.getMessage() +
                                    ": details: [" + mbean.getCanonicalName() + "]");
                        }
                    }

                    if (validRecord) {
                        record.setAttributes(mbeanStringMap);
                        metricsSet.add(record);
                    }
                } else {
                    getLogger().info("FILTERED: domain [" + domain + "] type [" + type + "]");
                }
            }
        } catch( ProcessException pe ) {
            throw pe;
        } catch( IOException ioe) {
            getLogger().error( "Exception connecting to JMX RMI Listener: " + ioe.getMessage() + ": hostname [" +
                    hostname + "] port [" + port + "]");
        } catch( SecurityException se ) {
          getLogger().error( "Exception connecting to JMX RMI Listener due to security issues: " +
                    se.getMessage() + ": hostname [" + hostname + "] port [" + port + "]" );
        } catch( InstanceNotFoundException|IntrospectionException|ReflectionException e ) {
            getLogger().error( "Exception with MBean Server: " + e.getMessage() );
        } catch( Exception e ) {
            getLogger().error( "Exception performing MBean Query: " + e.getMessage() );
        } 

        return metricsSet;
    }




    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        if (queue.size() < context.getProperty(BATCH_SIZE).asInteger()) {
            final long pollingMillis = context.getProperty(POLLING_INTERVAL).asTimePeriod(TimeUnit.SECONDS) * 1000;

            if ((queueLastUpdated.get() < System.currentTimeMillis() - pollingMillis) && listingLock.tryLock()) {
                try {
                    final Set<Record> metrics = performQuery( context );

                    queueLock.lock();

                    try {
                        metrics.removeAll(inProcess);
                        metrics.removeAll(recentlyProcessed);

                        queue.clear();
                        queue.addAll(metrics);

                        queueLastUpdated.set(System.currentTimeMillis());
                        recentlyProcessed.clear();

                        if (metrics.isEmpty()) {
                            context.yield();
                        }
                    } finally {
                        queueLock.unlock();
                    }
                } finally {
                    listingLock.unlock();
                }
            }
        }

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final List<Record> jmxList = new ArrayList<>(batchSize);
        queueLock.lock();
        try {
            queue.drainTo(jmxList, batchSize);
            if (jmxList.isEmpty()) {
                return;
            } else {
                inProcess.addAll(jmxList);
            }
        } finally {
            queueLock.unlock();
        }

        final ListIterator<Record> itr = jmxList.listIterator();

        FlowFile flowFile = null;
        try {
            while (itr.hasNext()) {
                final Record record = itr.next();

                final Map<String, String> attributes = new HashMap<>();
                final Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                attributes.put(HOSTNAME_ATTRIBUTE, context.getProperty(HOSTNAME).getValue());
                attributes.put(PORT_ATTRIBUTE, context.getProperty(PORT).getValue());
                attributes.put(TIMESTAMP_ATTRIBUTE, timestamp.toString());

                flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, attributes);
                getLogger().info("ATTRIBUTES: " + context.getProperty(HOSTNAME).getValue() + " " +
                        context.getProperty(PORT).getValue() + " " + timestamp.toString());

                flowFile = session.write(flowFile,new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        try {
                            out.write(record.toString().getBytes());
                        } catch (Exception e) {
                            getLogger().error("Exception writing metric record to flowfile: " + e.getMessage() +
                                    ": record content: [" + record.toString() + "]" );
                        }
                    }
                });

                session.getProvenanceReporter().create(flowFile);

                session.transfer(flowFile, REL_SUCCESS);
                getLogger().info("Added {} to flow", new Object[]{flowFile});

                if (!isScheduled()) {  // if processor stopped, put the rest of the files back on the queue.
                    queueLock.lock();
                    try {
                        while (itr.hasNext()) {
                            final Record nextRecord = itr.next();
                            queue.add(nextRecord);
                            inProcess.remove(nextRecord);
                        }
                    } finally {
                        queueLock.unlock();
                    }
                }
            }
            session.commit();
        } catch (final Exception e) {
            getLogger().error( "Exception fetching records: " + e.getMessage() );

            // anything that we've not already processed needs to be put back on the queue
            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
                session.commit();
            }
        } finally {
            queueLock.lock();
            try {
                inProcess.removeAll(jmxList);
                recentlyProcessed.addAll(jmxList);
            } finally {
                queueLock.unlock();
            }
        }
    }

    private boolean canProcess( String domain, String type ) {
        if (blackListFilter.isInList(ListFilter.BLACKLIST, domain, type )) {
            return false;
        }

        return (whiteListFilter.isInList(ListFilter.WHITELIST, domain, type )) ? true : false;
    }
}
