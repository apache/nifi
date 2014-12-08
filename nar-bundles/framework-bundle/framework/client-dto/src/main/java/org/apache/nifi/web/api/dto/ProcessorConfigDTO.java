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
package org.apache.nifi.web.api.dto;

import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlType;

/**
 * Configuration details for a processor in this NiFi.
 */
@XmlType(name = "processorConfig")
public class ProcessorConfigDTO {

    private Map<String, String> properties;
    private Map<String, PropertyDescriptorDTO> descriptors;

    // settings
    private String schedulingPeriod;
    private String schedulingStrategy;
    private String penaltyDuration;
    private String yieldDuration;
    private String bulletinLevel;
    private Long runDurationMillis;
    private Integer concurrentlySchedulableTaskCount;
    private Set<String> autoTerminatedRelationships;
    private String comments;
    private String customUiUrl;
    private Boolean lossTolerant;

    // annotation data
    private String annotationData;

    private Map<String, String> defaultConcurrentTasks;
    private Map<String, String> defaultSchedulingPeriod;

    public ProcessorConfigDTO() {

    }

    /**
     * The amount of time that should elapse between task executions. This will
     * not affect currently scheduled tasks.
     *
     * @return The scheduling period in seconds
     */
    public String getSchedulingPeriod() {
        return schedulingPeriod;
    }

    public void setSchedulingPeriod(String setSchedulingPeriod) {
        this.schedulingPeriod = setSchedulingPeriod;
    }

    /**
     * Indicates whether the processor should be scheduled to run in
     * event-driven mode or timer-driven mode
     *
     * @return
     */
    public String getSchedulingStrategy() {
        return schedulingStrategy;
    }

    public void setSchedulingStrategy(String schedulingStrategy) {
        this.schedulingStrategy = schedulingStrategy;
    }

    /**
     * The amount of time that is used when this processor penalizes a flow
     * file.
     *
     * @return
     */
    public String getPenaltyDuration() {
        return penaltyDuration;
    }

    public void setPenaltyDuration(String penaltyDuration) {
        this.penaltyDuration = penaltyDuration;
    }

    /**
     * When yielding, this amount of time must elaspe before this processor is
     * scheduled again.
     *
     * @return
     */
    public String getYieldDuration() {
        return yieldDuration;
    }

    public void setYieldDuration(String yieldDuration) {
        this.yieldDuration = yieldDuration;
    }

    /**
     * The level at this this processor will report bulletins.
     *
     * @return
     */
    public String getBulletinLevel() {
        return bulletinLevel;
    }

    public void setBulletinLevel(String bulletinLevel) {
        this.bulletinLevel = bulletinLevel;
    }

    /**
     * The number of tasks that should be concurrently scheduled for this
     * processor. If this processor doesn't allow parallel processing then any
     * positive input will be ignored.
     *
     * @return The concurrently schedulable task count
     */
    public Integer getConcurrentlySchedulableTaskCount() {
        return concurrentlySchedulableTaskCount;
    }

    public void setConcurrentlySchedulableTaskCount(Integer concurrentlySchedulableTaskCount) {
        this.concurrentlySchedulableTaskCount = concurrentlySchedulableTaskCount;
    }

    /**
     * Whether or not this Processor is Loss Tolerant
     *
     * @return
     */
    public Boolean isLossTolerant() {
        return lossTolerant;
    }

    public void setLossTolerant(final Boolean lossTolerant) {
        this.lossTolerant = lossTolerant;
    }

    /**
     * The comments for this processor.
     *
     * @return The comments
     */
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * The properties for this processor. Properties whose value is not set will
     * only contain the property name. These properties are (un)marshalled
     * differently since we need/want to control the ordering of the properties.
     * The descriptors and metadata are used as a lookup when processing these
     * properties.
     *
     * @return The optional properties
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * The descriptors for this processor's properties.
     *
     * @return
     */
    public Map<String, PropertyDescriptorDTO> getDescriptors() {
        return descriptors;
    }

    public void setDescriptors(Map<String, PropertyDescriptorDTO> descriptors) {
        this.descriptors = descriptors;
    }

    /**
     * Annotation data for this processor.
     *
     * @return The annotation data
     */
    public String getAnnotationData() {
        return annotationData;
    }

    public void setAnnotationData(String annotationData) {
        this.annotationData = annotationData;
    }

    /**
     * Whether of not this processor has a custom UI.
     *
     * @return
     */
    public String getCustomUiUrl() {
        return customUiUrl;
    }

    public void setCustomUiUrl(String customUiUrl) {
        this.customUiUrl = customUiUrl;
    }

    /**
     * The names of all processor relationships that cause a flow file to be
     * terminated if the relationship is not connected to anything
     *
     * @return
     */
    public Set<String> getAutoTerminatedRelationships() {
        return autoTerminatedRelationships;
    }

    public void setAutoTerminatedRelationships(final Set<String> autoTerminatedRelationships) {
        this.autoTerminatedRelationships = autoTerminatedRelationships;
    }

    /**
     * Maps default values for concurrent tasks for each applicable scheduling
     * strategy.
     *
     * @return
     */
    public Map<String, String> getDefaultConcurrentTasks() {
        return defaultConcurrentTasks;
    }

    public void setDefaultConcurrentTasks(Map<String, String> defaultConcurrentTasks) {
        this.defaultConcurrentTasks = defaultConcurrentTasks;
    }

    /**
     * The run duration in milliseconds.
     *
     * @return
     */
    public Long getRunDurationMillis() {
        return runDurationMillis;
    }

    public void setRunDurationMillis(Long runDurationMillis) {
        this.runDurationMillis = runDurationMillis;
    }

    /**
     * Maps default values for scheduling period for each applicable scheduling
     * strategy.
     *
     * @return
     */
    public Map<String, String> getDefaultSchedulingPeriod() {
        return defaultSchedulingPeriod;
    }

    public void setDefaultSchedulingPeriod(Map<String, String> defaultSchedulingPeriod) {
        this.defaultSchedulingPeriod = defaultSchedulingPeriod;
    }

    /**
     * The allowable values for a property with a constrained set of options.
     */
    @XmlType(name = "allowableValue")
    public static class AllowableValueDTO {

        private String displayName;
        private String value;
        private String description;

        /**
         * Returns the human-readable value that is allowed for this
         * PropertyDescriptor
         *
         * @return
         */
        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        /**
         * Returns the value for this allowable value.
         *
         * @return
         */
        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        /**
         * Returns a description of this Allowable Value, or <code>null</code>
         * if no description is given
         *
         * @return
         */
        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }

            if (!(obj instanceof AllowableValueDTO)) {
                return false;
            }

            final AllowableValueDTO other = (AllowableValueDTO) obj;
            return (this.value.equals(other.getValue()));
        }

        @Override
        public int hashCode() {
            return 23984731 + 17 * value.hashCode();
        }
    }

    /**
     * A description of a processor property.
     */
    @XmlType(name = "propertyDescriptor")
    public static class PropertyDescriptorDTO {

        private String name;
        private String displayName;
        private String description;
        private String defaultValue;
        private Set<AllowableValueDTO> allowableValues;
        private boolean required;
        private boolean sensitive;
        private boolean dynamic;
        private boolean supportsEl;

        /**
         * The set of allowable values for this property. If empty then the
         * allowable values are not constrained.
         *
         * @return
         */
        public Set<AllowableValueDTO> getAllowableValues() {
            return allowableValues;
        }

        public void setAllowableValues(Set<AllowableValueDTO> allowableValues) {
            this.allowableValues = allowableValues;
        }

        /**
         * The default value for this property.
         *
         * @return
         */
        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        /**
         * And explanation of the meaning of the given property. This
         * description is meant to be displayed to a user or simply provide a
         * mechanism of documenting intent.
         *
         * @return
         */
        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        /**
         * The property name.
         *
         * @return
         */
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        /**
         * The human-readable name to display to users.
         *
         * @return
         */
        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        /**
         * Determines whether the property is required for this processor.
         *
         * @return
         */
        public boolean isRequired() {
            return required;
        }

        public void setRequired(boolean required) {
            this.required = required;
        }

        /**
         * Indicates that the value for this property should be considered
         * sensitive and protected whenever stored or represented.
         *
         * @return
         */
        public boolean isSensitive() {
            return sensitive;
        }

        public void setSensitive(boolean sensitive) {
            this.sensitive = sensitive;
        }

        /**
         * Indicates whether this property is dynamic.
         *
         * @return
         */
        public boolean isDynamic() {
            return dynamic;
        }

        public void setDynamic(boolean dynamic) {
            this.dynamic = dynamic;
        }

        /**
         * Specifies whether or not this property support expression language.
         *
         * @return
         */
        public boolean getSupportsEl() {
            return supportsEl;
        }

        public void setSupportsEl(boolean supportsEl) {
            this.supportsEl = supportsEl;
        }
    }

}
