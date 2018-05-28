/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.influxdb;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class WriteOptions implements Cloneable {

    public static final TimeUnit PRECISION_DEFAULT = TimeUnit.NANOSECONDS;
    public static final MissingItemsBehaviour MISSING_FIELDS_BEHAVIOUR_DEFAULT = MissingItemsBehaviour.IGNORE;
    public static final MissingItemsBehaviour MISSING_TAGS_BEHAVIOUR_DEFAULT = MissingItemsBehaviour.IGNORE;
    public static final ComplexFieldBehaviour COMPLEX_FIELD_BEHAVIOUR_DEFAULT = ComplexFieldBehaviour.TEXT;
    public static final NullValueBehaviour NULL_FIELD_VALUE_BEHAVIOUR_DEFAULT = NullValueBehaviour.IGNORE;
    public static final String DEFAULT_RETENTION_POLICY = "autogen";

    private String database;
    private String retentionPolicy;
    private String timestamp;
    private TimeUnit precision = PRECISION_DEFAULT;
    private String measurement;
    private List<String> fields = new ArrayList<>();
    private MissingItemsBehaviour missingFields = MISSING_FIELDS_BEHAVIOUR_DEFAULT;
    private List<String> tags = new ArrayList<>();
    private MissingItemsBehaviour missingTags = MISSING_TAGS_BEHAVIOUR_DEFAULT;
    private ComplexFieldBehaviour complexFieldBehaviour = COMPLEX_FIELD_BEHAVIOUR_DEFAULT;
    private NullValueBehaviour nullValueBehaviour = NULL_FIELD_VALUE_BEHAVIOUR_DEFAULT;

    /**
     * @see PutInfluxDBRecord#MISSING_FIELD_BEHAVIOR
     * @see PutInfluxDBRecord#MISSING_TAG_BEHAVIOR
     */
    public enum MissingItemsBehaviour {

        /**
         * @see PutInfluxDBRecord#MISSING_ITEMS_BEHAVIOUR_IGNORE
         */
        IGNORE,

        /**
         * @see PutInfluxDBRecord#MISSING_ITEMS_BEHAVIOUR_FAIL
         */
        FAIL,
    }

    public enum ComplexFieldBehaviour {

        /**
         * @see PutInfluxDBRecord#COMPLEX_FIELD_IGNORE
         */
        IGNORE,

        /**
         * @see PutInfluxDBRecord#COMPLEX_FIELD_FAIL
         */
        FAIL,

        /**
         * @see PutInfluxDBRecord#COMPLEX_FIELD_WARN
         */
        WARN,

        /**
         * @see PutInfluxDBRecord#COMPLEX_FIELD_VALUE
         */
        TEXT,
    }

    public enum NullValueBehaviour {
        /**
         * @see PutInfluxDBRecord#NULL_VALUE_BEHAVIOUR_IGNORE
         */
        IGNORE,

        /**
         * @see PutInfluxDBRecord#NULL_VALUE_BEHAVIOUR_FAIL
         */
        FAIL
    }


    /**
     * @param database Name of database
     * @return immutable instance
     * @see PutInfluxDBRecord#DB_NAME
     */
    @NonNull
    public WriteOptions database(@NonNull final String database) {

        Objects.requireNonNull(database, "Database name is required");

        WriteOptions clone = clone();
        clone.database = database;

        return clone;
    }

    /**
     * @param retentionPolicy Name of retention policy
     * @return immutable instance
     * @see PutInfluxDBRecord#RETENTION_POLICY
     */
    @NonNull
    public WriteOptions setRetentionPolicy(@NonNull final String retentionPolicy) {

        Objects.requireNonNull(retentionPolicy, "Retention policy is required");

        WriteOptions clone = clone();
        clone.retentionPolicy = retentionPolicy;

        return clone;
    }

    /**
     * @param timestamp A name of the record field that used as a 'timestamp'
     * @return immutable instance
     * @see PutInfluxDBRecord#TIMESTAMP_FIELD
     */
    @NonNull
    public WriteOptions timestamp(@Nullable final String timestamp) {

        WriteOptions clone = clone();
        clone.timestamp = timestamp;

        return clone;
    }

    /**
     * @param precision Precision of timestamp
     * @return immutable instance
     * @see PutInfluxDBRecord#TIMESTAMP_PRECISION
     */
    @NonNull
    public WriteOptions precision(@NonNull final TimeUnit precision) {

        Objects.requireNonNull(precision, "Precision of timestamp is required");

        WriteOptions clone = clone();
        clone.precision = precision;

        return clone;
    }

    /**
     * @param measurement Name of the measurement
     * @return immutable instance
     * @see PutInfluxDBRecord#MEASUREMENT
     */
    @NonNull
    public WriteOptions measurement(@NonNull final String measurement) {

        Objects.requireNonNull(measurement, "Name of the measurement is required");

        WriteOptions clone = clone();
        clone.measurement = measurement;

        return clone;
    }

    /**
     * @param fields Name of the fields
     * @return immutable instance
     * @see PutInfluxDBRecord#FIELDS
     */
    @NonNull
    public WriteOptions fields(@NonNull final List<String> fields) {

        Objects.requireNonNull(fields, "Fields are required");

        WriteOptions clone = clone();
        clone.fields.addAll(fields);

        return clone;
    }

    /**
     * @param missingFields Missing fields behaviour
     * @return immutable instance
     * @see PutInfluxDBRecord#MISSING_FIELD_BEHAVIOR
     */
    @NonNull
    public WriteOptions missingFields(@NonNull final MissingItemsBehaviour missingFields) {

        Objects.requireNonNull(missingFields, "Missing fields behaviour is required");

        WriteOptions clone = clone();
        clone.missingFields = missingFields;

        return clone;
    }

    /**
     * @param tags Evaluated names of the tags
     * @return immutable instance
     * @see PutInfluxDBRecord#TAGS
     */
    @NonNull
    public WriteOptions tags(@NonNull final List<String> tags) {

        Objects.requireNonNull(tags, "Tags are required");

        WriteOptions clone = clone();
        clone.tags.addAll(tags);

        return clone;
    }

    /**
     * @param missingTags Missing tags behaviour
     * @return immutable instance
     * @see PutInfluxDBRecord#MISSING_TAG_BEHAVIOR
     */
    @NonNull
    public WriteOptions missingTags(@NonNull final MissingItemsBehaviour missingTags) {

        Objects.requireNonNull(missingTags, "Missing tags behaviour is required");

        WriteOptions clone = clone();
        clone.missingTags = missingTags;

        return clone;
    }

    /**
     * @param complexFieldBehaviour Complex field behaviour
     * @return immutable instance
     * @see PutInfluxDBRecord#COMPLEX_FIELD_BEHAVIOR
     */
    @NonNull
    public WriteOptions complexFieldBehaviour(@NonNull final ComplexFieldBehaviour complexFieldBehaviour) {

        Objects.requireNonNull(complexFieldBehaviour, "Missing tags behaviour is required");

        WriteOptions clone = clone();
        clone.complexFieldBehaviour = complexFieldBehaviour;

        return clone;
    }

    /**
     * @param nullValueBehaviour Null Value Behaviour
     * @return immutable instance
     * @see PutInfluxDBRecord#NULL_VALUE_BEHAVIOR
     */
    @NonNull
    public WriteOptions nullValueBehaviour(@NonNull final NullValueBehaviour nullValueBehaviour) {

        Objects.requireNonNull(nullValueBehaviour, "Null Value Behavior is required");

        WriteOptions clone = clone();
        clone.nullValueBehaviour = nullValueBehaviour;

        return clone;
    }

    /**
     * @see PutInfluxDBRecord#DB_NAME
     */
    @NonNull
    public String getDatabase() {
        return database;
    }

    /**
     * @see PutInfluxDBRecord#RETENTION_POLICY
     */
    @NonNull
    public String getRetentionPolicy() {
        return retentionPolicy;
    }

    /**
     * @return Evaluated timestamp name
     * @see PutInfluxDBRecord#TIMESTAMP_FIELD
     */
    @Nullable
    public String getTimestamp() {

        return timestamp;
    }

    /**
     * @return Evaluated timestamp precision
     * @see PutInfluxDBRecord#TIMESTAMP_PRECISION
     */
    @NonNull
    public TimeUnit getPrecision() {
        return precision;
    }

    /**
     * @return Evaluated measurement name
     * @see PutInfluxDBRecord#MEASUREMENT
     */
    @NonNull
    public String getMeasurement() {
        return measurement;
    }

    /**
     * @return Evaluated fields names
     * @see PutInfluxDBRecord#FIELDS
     */
    @NonNull
    public List<String> getFields() {
        return fields;
    }

    /**
     * @see PutInfluxDBRecord#MISSING_FIELD_BEHAVIOR
     */
    @NonNull
    public MissingItemsBehaviour getMissingFields() {
        return missingFields;
    }

    /**
     * @return Evaluated tags names
     * @see PutInfluxDBRecord#TAGS
     */
    @NonNull
    public List<String> getTags() {
        return tags;
    }

    /**
     * @see PutInfluxDBRecord#MISSING_TAG_BEHAVIOR
     */
    @NonNull
    public MissingItemsBehaviour getMissingTags() {
        return missingTags;
    }

    /**
     * @see PutInfluxDBRecord#COMPLEX_FIELD_BEHAVIOR
     */
    @NonNull
    public ComplexFieldBehaviour getComplexFieldBehaviour() {
        return complexFieldBehaviour;
    }

    /**
     * @see PutInfluxDBRecord#NULL_VALUE_BEHAVIOR
     */
    @NonNull
    public NullValueBehaviour getNullValueBehaviour() {
        return nullValueBehaviour;
    }

    @Override
    protected WriteOptions clone() {

        try {
            return (WriteOptions) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
