package org.apache.nifi.processors.standard;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.OnScheduled;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"hold", "release", "signal"})
@CapabilityDescription("Holds incoming flow files until a matching signal flow file enters the processor.  "
		+ "Incoming files are classified as either held files or signals.  "
		+ "Held files are routed to the Held relationship until a matching signal has been received.")
public class HoldFile extends AbstractProcessor {
	public static final String FLOW_FILE_RELEASE_VALUE = "flow.file.release.value";
	public static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");
	
	public static final PropertyDescriptor MAX_SIGNAL_AGE = new PropertyDescriptor
			.Builder().name("Max Signal Age")
			.description("The maximum age of a signal that will trigger a file to be released.  "
					+ "Expected format is <duration> <time unit> where <duration> is a positive "
					+ "integer and <time unit> is one of seconds, minutes, hours")
			.required(true).defaultValue("24 hours")
			.addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
			.build();
	public static final PropertyDescriptor RELEASE_SIGNAL_ATTRIBUTE = new PropertyDescriptor
			.Builder().name("Release Signal Attribute")
			.description("The flow file attribute name on held files that will be checked against values in the signal cache.")
			.required(true).
			addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
			.build();
	public static final PropertyDescriptor COPY_SIGNAL_ATTRIBUTES = new PropertyDescriptor
			.Builder().name("Copy Signal Attributes?")
			.description("If true, a signal's flow file attributes will be copied to its matching held files, "
					+ "with the exception of flow.file.release.value and the configured Signal Failure Attribute")
			.required(true)
			.defaultValue("true")
			.allowableValues("true", "false")
			.addValidator(StandardValidators.BOOLEAN_VALIDATOR)
			.build();
	public static final PropertyDescriptor FAILURE_ATTRIBUTE = new PropertyDescriptor
			.Builder().name("Signal Failure Attribute")
			.description("Signals that have this attribute set to 'true' "
					+ "will cause matching held flow files to route to Failure.  If this attribute "
					+ "is not populated, it is assumed that the flow file succeeds.")
			.required(false)
			.addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
			.build();
	
	public static final Relationship REL_HOLD = new Relationship.Builder()
			.name("hold").description("Held files whose signals have not been received are routed here").build();

	public static final Relationship REL_RELEASE = new Relationship.Builder()
			.name("release").description("Held files whose signals have been received are routed here").build();

	public static final Relationship REL_EXPIRED = new Relationship.Builder()
			.name("expired").description("Held files that expire are routed here").build();

	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure").description("Held files whose signal contains the Signal Failure Attribute are "
					+ "routed here, indicating a processing failure upstream").build();
	
	private Set<String> excludedAttributes = new HashSet<>();

	private Set<Relationship> relationships;
	private List<PropertyDescriptor> descriptors;

	private String failureAttribute;
	private volatile Map<String, ReleaseAttributes> releaseValues = new ConcurrentHashMap<>();
	private long expirationDuration = 0L;
	private String releaseSignalAttribute;
	private boolean copyAttributes;
	

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final Set<Relationship> relationships = new HashSet<>();
		relationships .add(REL_HOLD);
		relationships .add(REL_RELEASE);
		relationships .add(REL_EXPIRED);
		relationships .add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);

		final List<PropertyDescriptor> descriptors = new ArrayList<>();
		descriptors.add(RELEASE_SIGNAL_ATTRIBUTE);
		descriptors.add(MAX_SIGNAL_AGE);
		descriptors.add(COPY_SIGNAL_ATTRIBUTES);
		descriptors.add(FAILURE_ATTRIBUTE);
		this.descriptors = Collections.unmodifiableList(descriptors);
	}

	@Override
	public final Set<Relationship> getRelationships() { 
		return relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		this.expirationDuration = context.getProperty(MAX_SIGNAL_AGE)
				.asTimePeriod(TimeUnit.MILLISECONDS);
		this.releaseSignalAttribute = context.getProperty(
				RELEASE_SIGNAL_ATTRIBUTE).getValue();
		this.copyAttributes = context.getProperty(
				COPY_SIGNAL_ATTRIBUTES).asBoolean();
		PropertyValue failureAttrValue = context.getProperty(FAILURE_ATTRIBUTE);
		if (failureAttrValue != null) {
			this.failureAttribute = failureAttrValue.getValue();
		}

		this.excludedAttributes.clear();
		this.excludedAttributes.add(FLOW_FILE_RELEASE_VALUE);
		if (this.failureAttribute != null) {
			this.excludedAttributes.add(this.failureAttribute);
		}
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		int encountered = 0;
		FlowFile flowFile = null;
		while((flowFile = session.get()) != null) {
			if (StringUtils.isBlank(flowFile.getAttribute(FLOW_FILE_RELEASE_VALUE))) {
				this.processHoldFile(flowFile, session);
			} else {
				this.processSignal(flowFile, session);
			}
			encountered++;
		}	
	
		if (!isScheduled()) { 
			return; 
		}
		if (encountered == 0) { 
			context.yield();
		}
	}

	/**
	 * Stores a signal and all associated flow file attributes, as applicable.
	 * @param flowFile
	 * @param session
	 */
	private void processSignal(FlowFile flowFile, ProcessSession session) {
		ReleaseAttributes releaseAttributes = new ReleaseAttributes();
		// Store any propagated signal attributes
		if (this.copyAttributes) {
			releaseAttributes.attributes.putAll(flowFile.getAttributes());
		}

		// Check if the signal indicates a failure upstream
		String failureValue = flowFile.getAttribute(failureAttribute);
		releaseAttributes.failed = "true".equalsIgnoreCase(failureValue);
		String releaseValue = flowFile.getAttribute(FLOW_FILE_RELEASE_VALUE);
		releaseValues.put(releaseValue , releaseAttributes);
		if (getLogger().isDebugEnabled()) {
			getLogger().debug("{} is marking flow files with {}={} for release", 
					new Object[] {flowFile, releaseSignalAttribute, releaseValue });
		}
		
		session.remove(flowFile);
	}

	/**
	 * Transfers the held file to either Release (if a release signal has
	 * been found), Hold (if not), or Expired (if it has expired).
	 * @param flowFile
	 * @param session
	 */
	private void processHoldFile(FlowFile flowFile, ProcessSession session) {
		String attributeValue = flowFile.getAttribute(releaseSignalAttribute);

		// Do we have a matching attribute to be released?
		if (attributeValue != null && releaseValues.containsKey(attributeValue)) {
			ReleaseAttributes releaseAttributes = releaseValues
					.get(attributeValue);
			boolean copyAttributes = (!releaseAttributes.attributes.isEmpty() && this.copyAttributes);
			if (copyAttributes) {
				Map<String, String> signalAttributes = getNewAttributes(
						flowFile.getAttributes(), releaseAttributes.attributes,
						this.excludedAttributes);
				flowFile = session.putAllAttributes(flowFile, signalAttributes);
				flowFile = session.removeAllAttributes(flowFile,
						this.excludedAttributes);
			}
			if (getLogger().isDebugEnabled()) {
				getLogger().debug("{} was released.  Attributes copied? {}",
						new Object[] { flowFile, copyAttributes });
			}
			// Remove the signal
			synchronized (releaseValues) {
				releaseValues.remove(attributeValue);
				if (releaseAttributes.failed) {
					getLogger()
							.warn("Received a non-success value for {}, routing to failure: {}",
									new Object[] { failureAttribute, flowFile });
					session.transfer(flowFile, REL_FAILURE);
				} else {
					session.transfer(flowFile, REL_RELEASE);
				}
			}
		} else {
			// We don't have a signal yet. Let's check if it has expired
			long entryDate = flowFile.getEntryDate();
			if (isExpired(entryDate)) {
				session.transfer(flowFile, REL_EXPIRED);
				// It expired. We likely have some expired signals too, so let's
				// check
				synchronized (releaseValues) {
					for (Iterator<Entry<String, ReleaseAttributes>> it = releaseValues
							.entrySet().iterator(); it.hasNext();) {
						Entry<String, ReleaseAttributes> entry = it.next();
						ReleaseAttributes releaseAttributes = entry.getValue();
						if (isExpired(releaseAttributes.creationTimestamp)) {
							releaseAttributes.attributes.clear();
							it.remove();
						}
					}
				}
				if (getLogger().isDebugEnabled()) {
					getLogger().debug("Expiring {}", new Object[] { flowFile });
				}
			} else {
				// It hasn't expired, so transfer to Hold
				session.transfer(flowFile, REL_HOLD);
				if (getLogger().isTraceEnabled()) {
					getLogger().trace("Holding {}", new Object[] { flowFile });
				}
			}
		}
	}

	// Compares two maps and returns only those attributes that are new
	private static Map<String, String> getNewAttributes(
			Map<String, String> existingAttributes,
			Map<String, String> attributesToAdd, Set<String> alwaysOverrideKeys) {
		Map<String, String> newAttributes = new HashMap<>();
		for (Entry<String, String> entryToAdd : attributesToAdd.entrySet()) {
			if (!existingAttributes.containsKey(entryToAdd.getKey())
					|| alwaysOverrideKeys.contains(entryToAdd.getKey())) {
				newAttributes.put(entryToAdd.getKey(), entryToAdd.getValue());
			}
		}
		return newAttributes;
	}

	// Returns true if the given time is far enough in the past to expire
	private boolean isExpired(Long creationTimestamp) {
		return creationTimestamp + expirationDuration < System.currentTimeMillis();
	}

	public static class ReleaseAttributes {
		private long creationTimestamp = System.currentTimeMillis();
		private boolean failed;
		private Map<String, String> attributes = new HashMap<>();
	}

}