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
package org.apache.nifi.components;

import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceDefinition;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.StandardResourceReferenceFactory;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.components.resource.StandardResourceDefinition;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * An immutable object for holding information about a type of component
 * property.
 *
 */
public final class PropertyDescriptor implements Comparable<PropertyDescriptor> {

    public static final PropertyDescriptor NULL_DESCRIPTOR = new PropertyDescriptor.Builder().name("").build();

    /**
     * The proper name for the property. This is the primary mechanism of
     * comparing equality.
     */
    private final String name;

    /**
     * The name that should be displayed to user when referencing this property
     */
    private final String displayName;

    /**
     * And explanation of the meaning of the given property. This description is
     * meant to be displayed to a user or simply provide a mechanism of
     * documenting intent.
     */
    private final String description;
    /**
     * The default value for this property
     */
    private final String defaultValue;
    /**
     * The allowable values for this property. If empty then the allowable
     * values are not constrained
     */
    private final List<AllowableValue> allowableValues;
    /**
     * Determines whether the property is required for this processor
     */
    private final boolean required;
    /**
     * indicates that the value for this property should be considered sensitive
     * and protected whenever stored or represented
     */
    private final boolean sensitive;
    /**
     * indicates whether this property well-known for this processor or is
     * user-defined
     */
    private final boolean dynamic;
    /**
     * indicates whether or not this property supports the Attribute Expression
     * Language
     */
    @Deprecated
    private final boolean expressionLanguageSupported;
    /**
     * indicates whether or nor this property will evaluate expression language
     * against the flow file attributes
     */
    private final ExpressionLanguageScope expressionLanguageScope;
    /**
     * indicates whether or not this property represents resources that should be added
     * to the classpath and used for loading native libraries for this instance of the component
     */
    private final boolean dynamicallyModifiesClasspath;

    /**
     * the interface of the {@link ControllerService} that this Property refers
     * to; will be null if not identifying a ControllerService.
     */
    private final Class<? extends ControllerService> controllerServiceDefinition;

    /**
     * The validators that should be used whenever an attempt is made to set
     * this property value. Any allowable values specified will be checked first
     * and any validators specified will be ignored.
     */
    private final List<Validator> validators;

    /**
     * The list of dependencies that this property has on other properties
     */
    private final Set<PropertyDependency> dependencies;

    /**
     * The definition of the resource(s) that this property references
     */
    private final ResourceDefinition resourceDefinition;

    protected PropertyDescriptor(final Builder builder) {
        this.displayName = builder.displayName == null ? builder.name : builder.displayName;
        this.name = builder.name;
        this.description = builder.description;
        this.defaultValue = builder.defaultValue;
        this.allowableValues = builder.allowableValues == null ? null : Collections.unmodifiableList(new ArrayList<>(builder.allowableValues));
        this.required = builder.required;
        this.sensitive = builder.sensitive;
        this.dynamic = builder.dynamic;
        this.dynamicallyModifiesClasspath = builder.dynamicallyModifiesClasspath;
        this.expressionLanguageSupported = builder.expressionLanguageSupported;
        this.expressionLanguageScope = builder.expressionLanguageScope;
        this.controllerServiceDefinition = builder.controllerServiceDefinition;
        this.validators = Collections.unmodifiableList(new ArrayList<>(builder.validators));
        this.dependencies = builder.dependencies == null ? Collections.emptySet() : Collections.unmodifiableSet(new HashSet<>(builder.dependencies));
        this.resourceDefinition = builder.resourceDefinition;
    }

    @Override
    public int compareTo(final PropertyDescriptor o) {
        if (o == null) {
            return -1;
        }
        return getName().compareTo(o.getName());
    }

    /**
     * Validates the given input against this property descriptor's validator.
     * If this descriptor has a set of allowable values then the given value is
     * only checked against the allowable values.
     *
     * @param input the value to validate
     * @param context the context of validation
     * @return the result of validating the input
     */
    public ValidationResult validate(final String input, final ValidationContext context) {
        ValidationResult lastResult = Validator.INVALID.validate(this.name, input, context);

        if (allowableValues != null && !allowableValues.isEmpty()) {
            final ConstrainedSetValidator csValidator = new ConstrainedSetValidator(allowableValues);
            final ValidationResult csResult = csValidator.validate(this.name, input, context);

            if (csResult.isValid()) {
                lastResult = csResult;
            } else {
                return csResult;
            }
        }

        final ResourceDefinition resourceDefinition = getResourceDefinition();
        if (resourceDefinition != null) {
            final Validator validator = new ResourceDefinitionValidator(resourceDefinition, this.expressionLanguageScope);
            final ValidationResult result = validator.validate(this.name, input, context);
            if (!result.isValid()) {
                return result;
            }

            lastResult = result;
        }

        for (final Validator validator : validators) {
            lastResult = validator.validate(this.name, input, context);
            if (!lastResult.isValid()) {
                break;
            }
        }

        if (getControllerServiceDefinition() != null) {
            final ControllerService service = context.getControllerServiceLookup().getControllerService(input);
            if (service == null) {
                return new ValidationResult.Builder()
                    .input(input)
                    .subject(getDisplayName())
                    .valid(false)
                    .explanation("Property references a Controller Service that does not exist")
                    .build();
            } else {
                return new ValidationResult.Builder()
                    .valid(true)
                    .build();
            }
        }

        return lastResult;
    }


    public static final class Builder {

        private String displayName = null;
        private String name = null;
        private String description = "";
        private String defaultValue = null;
        private List<AllowableValue> allowableValues = null;
        private Set<PropertyDependency> dependencies = null;
        private boolean required = false;
        private boolean sensitive = false;

        @Deprecated
        private boolean expressionLanguageSupported = false;

        private ExpressionLanguageScope expressionLanguageScope = ExpressionLanguageScope.NONE;
        private boolean dynamic = false;
        private boolean dynamicallyModifiesClasspath = false;
        private Class<? extends ControllerService> controllerServiceDefinition;
        private ResourceDefinition resourceDefinition;
        private List<Validator> validators = new ArrayList<>();

        public Builder fromPropertyDescriptor(final PropertyDescriptor specDescriptor) {
            this.name = specDescriptor.name;
            this.displayName = specDescriptor.displayName;
            this.description = specDescriptor.description;
            this.defaultValue = specDescriptor.defaultValue;
            this.allowableValues = specDescriptor.allowableValues == null ? null : new ArrayList<>(specDescriptor.allowableValues);
            this.required = specDescriptor.required;
            this.sensitive = specDescriptor.sensitive;
            this.dynamic = specDescriptor.dynamic;
            this.dynamicallyModifiesClasspath = specDescriptor.dynamicallyModifiesClasspath;
            this.expressionLanguageSupported = specDescriptor.expressionLanguageSupported;
            this.expressionLanguageScope = specDescriptor.expressionLanguageScope;
            this.controllerServiceDefinition = specDescriptor.getControllerServiceDefinition();
            this.validators = new ArrayList<>(specDescriptor.validators);
            this.dependencies = new HashSet<>(specDescriptor.dependencies);
            this.resourceDefinition = specDescriptor.resourceDefinition;
            return this;
        }

        /**
         * Sets a unique id for the property. This field is optional and if not
         * specified the PropertyDescriptor's name will be used as the
         * identifying attribute. However, by supplying an id, the
         * PropertyDescriptor's name can be changed without causing problems.
         * This is beneficial because it allows a User Interface to represent
         * the name differently.
         *
         * @param displayName of the property
         * @return the builder
         */
        public Builder displayName(final String displayName) {
            if (null != displayName) {
                this.displayName = displayName;
            }

            return this;
        }

        /**
         * Sets the property name.
         *
         * @param name of the property
         * @return the builder
         */
        public Builder name(final String name) {
            if (null != name) {
                this.name = name;
            }
            return this;
        }

        /**
         * Sets the value indicating whether or not this Property will support
         * the Attribute Expression Language.
         *
         * @param supported true if yes; false otherwise
         * @return the builder
         */
        @Deprecated
        public Builder expressionLanguageSupported(final boolean supported) {
            this.expressionLanguageSupported = supported;
            return this;
        }

        /**
         * Sets the scope of the expression language evaluation
         *
         * @param expressionLanguageScope scope of the expression language evaluation
         * @return the builder
         */
        public Builder expressionLanguageSupported(final ExpressionLanguageScope expressionLanguageScope) {
            this.expressionLanguageScope = expressionLanguageScope;
            return this;
        }

        /**
         * @param description of the property
         * @return the builder
         */
        public Builder description(final String description) {
            if (null != description) {
                this.description = description;
            }
            return this;
        }

        /**
         * Specifies the initial value and the default value that will be used
         * if the user does not specify a value. When {@link #build()} is
         * called, if Allowable Values have been set (see
         * {@link #allowableValues(AllowableValue...)}) and this value is not
         * one of those Allowable Values and Exception will be thrown. If the
         * Allowable Values have been set using the
         * {@link #allowableValues(AllowableValue...)} method, the default value
         * should be set to the "Value" of the {@link AllowableValue} object
         * (see {@link AllowableValue#getValue()}).
         *
         * @param value default value
         * @return the builder
         */
        public Builder defaultValue(final String value) {
            if (null != value) {
                this.defaultValue = value;
            }
            return this;
        }

        public Builder dynamic(final boolean dynamic) {
            this.dynamic = dynamic;
            return this;
        }

        /**
         * Specifies that the value of this property represents one or more resources that the
         * framework should add to the classpath of as well as consider when looking for native
         * libraries for the given component.
         * <p/>
         * NOTE: If a component contains a PropertyDescriptor where dynamicallyModifiesClasspath is set to true,
         *  the component may also be annotated with @RequiresInstanceClassloading, in which case every class will
         *  be loaded by a separate InstanceClassLoader for each processor instance.<br/>
         *  It also allows to load native libraries from the extra classpath.
         *  <p/>
         *  One can chose to omit the annotation. In this case the loading of native libraries from the extra classpath
         *  is not supported.
         *  Also by default, classes will be loaded by a common NarClassLoader, however it's possible to acquire an
         *  InstanceClassLoader by calling Thread.currentThread().getContextClassLoader() which can be used manually
         *  to load required classes on an instance-by-instance basis
         *  (by calling {@link Class#forName(String, boolean, ClassLoader)} for example).
         *
         * Any property descriptor that dynamically modifies the classpath should also make use of the {@link #identifiesExternalResource(ResourceCardinality, ResourceType, ResourceType...)} method
         * to indicate that the property descriptor references external resources and optionally restrict which types of resources and how many resources the property allows.
         *
         * @param dynamicallyModifiesClasspath whether or not this property should be used by the framework to modify the classpath
         * @return the builder
         */
        public Builder dynamicallyModifiesClasspath(final boolean dynamicallyModifiesClasspath) {
            this.dynamicallyModifiesClasspath = dynamicallyModifiesClasspath;
            return this;
        }

        /**
         * @param values contrained set of values
         * @return the builder
         */
        public Builder allowableValues(final Set<String> values) {
            if (null != values) {
                this.allowableValues = new ArrayList<>();

                for (final String value : values) {
                    this.allowableValues.add(new AllowableValue(value, value));
                }
            }
            return this;
        }

        public <E extends Enum<E>> Builder allowableValues(final E[] values) {
            if (null != values) {
                this.allowableValues = new ArrayList<>();
                for (final E value : values) {
                    allowableValues.add(new AllowableValue(value.name(), value.name()));
                }
            }
            return this;
        }

        /**
         * @param values constrained set of values
         * @return the builder
         */
        public Builder allowableValues(final String... values) {
            if (null != values) {
                this.allowableValues = new ArrayList<>();
                for (final String value : values) {
                    allowableValues.add(new AllowableValue(value, value));
                }
            }
            return this;
        }

        /**
         * Sets the Allowable Values for this Property
         *
         * @param values contrained set of values
         * @return the builder
         */
        public Builder allowableValues(final AllowableValue... values) {
            if (null != values) {
                this.allowableValues = Arrays.asList(values);
            }
            return this;
        }

        /**
         * @param required true if yes; false otherwise
         * @return the builder
         */
        public Builder required(final boolean required) {
            this.required = required;
            return this;
        }

        /**
         * @param sensitive true if sensitive; false otherwise
         * @return the builder
         */
        public Builder sensitive(final boolean sensitive) {
            this.sensitive = sensitive;
            return this;
        }

        /**
         * @param validator for the property
         * @return the builder
         */
        public Builder addValidator(final Validator validator) {
            if (validator != null) {
                validators.add(validator);
            }
            return this;
        }

        /**
         * Specifies that this property provides the identifier of a Controller
         * Service that implements the given interface
         *
         * @param controllerServiceDefinition the interface that is implemented
         * by the Controller Service
         * @return the builder
         */
        public Builder identifiesControllerService(final Class<? extends ControllerService> controllerServiceDefinition) {
            if (controllerServiceDefinition != null) {
                this.controllerServiceDefinition = controllerServiceDefinition;
            }
            return this;
        }

        private boolean isValueAllowed(final String value) {
            if (allowableValues == null || value == null) {
                return true;
            }

            for (final AllowableValue allowableValue : allowableValues) {
                if (allowableValue.getValue().equals(value)) {
                    return true;
                }
            }

            return false;
        }

        /**
         * Specifies that this property references one or more resources that are external to NiFi that the component is meant to consume.
         * Any property descriptor that identifies an external resource will be automatically validated against the following rules:
         * <ul>
         *     <li>If the ResourceCardinality is SINGLE, the given property value must be a file, a directory, or a URL that uses a protocol of http/https/file.</li>
         *     <li>The given resourceTypes dictate which types of input are allowed. For example, if <code>identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)</code>
         *     is used, the input must be a regular file. If <code>identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.DIRECTORY)</code> is used, then the input
         *     must be exactly one file OR directory.
         *     </li>
         *     <li>If the ResourceCardinality is MULTIPLE, the given property value may consist of one or more resources, each separted by a comma and optional white space.</li>
         * </ul>
         *
         * Generally, any property descriptor that makes use of the {@link #dynamicallyModifiesClasspath(boolean)} method to dynamically update its classpath should also
         * make use of this method, specifying which types of resources are allowed and how many.
         *
         * @param cardinality specifies how many resources the property should allow
         * @param resourceType the type of resource that is allowed
         * @param additionalResourceTypes if more than one type of resource is allowed, any resource type in addition to the given resource type may be provided
         * @return the builder
         */
        public Builder identifiesExternalResource(final ResourceCardinality cardinality, final ResourceType resourceType, final ResourceType... additionalResourceTypes) {
            Objects.requireNonNull(cardinality);
            Objects.requireNonNull(resourceType);

            final Set<ResourceType> resourceTypes = new HashSet<>();
            resourceTypes.add(resourceType);
            resourceTypes.addAll(Arrays.asList(additionalResourceTypes));

            this.resourceDefinition = new StandardResourceDefinition(cardinality, resourceTypes);
            return this;
        }

        /**
         * Establishes a relationship between this Property and the given property by declaring that this Property is only relevant if the given Property has a non-null value.
         * Furthermore, if one or more explicit Allowable Values are provided, this Property will not be relevant unless the given Property's value is equal to one of the given Allowable Values.
         * If this method is called multiple times, each with a different dependency, then a relationship is established such that this Property is relevant only if all dependencies are satisfied.
         *
         * In the case that this property is NOT considered to be relevant (meaning that it depends on a property whose value is not specified, or whose value does not match one of the given
         * Allowable Values), the property will not be shown in the component's configuration in the User Interface. Additionally, this property's value will not be considered for
         * validation. That is, if this property is configured with an invalid value and this property depends on Property Foo, and Property Foo does not have a value set, then the component
         * will still be valid, because the value of this property is irrelevant.
         *
         * If the given property is not relevant (because its dependencies are not satisfied), this property is also considered not to be valid.
         *
         * @param property the property that must be set in order for this property to become relevant
         * @param dependentValues the possible values for the given property for which this Property is relevant
         * @return the builder
         */
        public Builder dependsOn(final PropertyDescriptor property, final AllowableValue... dependentValues) {
            if (dependencies == null) {
                dependencies = new HashSet<>();
            }

            if (dependentValues.length == 0) {
                dependencies.add(new PropertyDependency(property.getName(), property.getDisplayName()));
            } else {
                final Set<String> dependentValueSet = new HashSet<>();
                for (final AllowableValue value : dependentValues) {
                    dependentValueSet.add(value.getValue());
                }

                dependencies.add(new PropertyDependency(property.getName(), property.getDisplayName(), dependentValueSet));
            }

            return this;
        }


        /**
         * Establishes a relationship between this Property and the given property by declaring that this Property is only relevant if the given Property has a value equal to one of the given
         * <code>String</code> arguments.
         * If this method is called multiple times, each with a different dependency, then a relationship is established such that this Property is relevant only if all dependencies are satisfied.
         *
         * In the case that this property is NOT considered to be relevant (meaning that it depends on a property whose value is not specified, or whose value does not match one of the given
         * Allowable Values), the property will not be shown in the component's configuration in the User Interface. Additionally, this property's value will not be considered for
         * validation. That is, if this property is configured with an invalid value and this property depends on Property Foo, and Property Foo does not have a value set, then the component
         * will still be valid, because the value of this property is irrelevant.
         *
         * If the given property is not relevant (because its dependencies are not satisfied), this property is also considered not to be valid.
         *
         * @param property the property that must be set in order for this property to become relevant
         * @param firstDependentValue the first value for the given property for which this Property is relevant
         * @param additionalDependentValues any other values for the given property for which this Property is relevant
         * @return the builder
         */
        public Builder dependsOn(final PropertyDescriptor property, final String firstDependentValue, final String... additionalDependentValues) {
            final AllowableValue[] dependentValues = new AllowableValue[additionalDependentValues.length + 1];
            dependentValues[0] = new AllowableValue(firstDependentValue);
            int i=1;
            for (final String additionalDependentValue : additionalDependentValues) {
                dependentValues[i++] = new AllowableValue(additionalDependentValue);
            }

            return dependsOn(property, dependentValues);
        }

        /**
         * @return a PropertyDescriptor as configured
         *
         * @throws IllegalStateException if allowable values are configured but
         * no default value is set, or the default value is not contained within
         * the allowable values.
         */
        public PropertyDescriptor build() {
            if (name == null) {
                throw new IllegalStateException("Must specify a name");
            }
            if (!isValueAllowed(defaultValue)) {
                throw new IllegalStateException("Default value [" + defaultValue + "] is not in the set of allowable values");
            }

            return new PropertyDescriptor(this);
        }
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public boolean isRequired() {
        return required;
    }

    public boolean isSensitive() {
        return sensitive;
    }

    public boolean isDynamic() {
        return dynamic;
    }

    public boolean isExpressionLanguageSupported() {
        return expressionLanguageSupported || !expressionLanguageScope.equals(ExpressionLanguageScope.NONE);
    }

    public ExpressionLanguageScope getExpressionLanguageScope() {
        return expressionLanguageScope;
    }

    public boolean isDynamicClasspathModifier() {
        return dynamicallyModifiesClasspath;
    }

    public Class<? extends ControllerService> getControllerServiceDefinition() {
        return controllerServiceDefinition;
    }

    public List<Validator> getValidators() {
        return validators;
    }

    public List<AllowableValue> getAllowableValues() {
        return allowableValues;
    }

    public Set<PropertyDependency> getDependencies() {
        return dependencies;
    }

    public ResourceDefinition getResourceDefinition() {
        return resourceDefinition;
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null) {
            return false;
        }
        if (!(other instanceof PropertyDescriptor)) {
            return false;
        }
        if (this == other) {
            return true;
        }

        final PropertyDescriptor desc = (PropertyDescriptor) other;
        return this.name.equals(desc.name);
    }

    @Override
    public int hashCode() {
        return 287 + this.name.hashCode() * 47;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + displayName + "]";
    }

    private static final class ConstrainedSetValidator implements Validator {

        private static final String POSITIVE_EXPLANATION = "Given value found in allowed set";
        private static final String NEGATIVE_EXPLANATION = "Given value not found in allowed set '%1$s'";
        private static final String VALUE_DEMARCATOR = ", ";
        private final String validStrings;
        private final Collection<String> validValues;

        /**
         * Constructs a validator that will check if the given value is in the
         * given set.
         *
         * @param validValues values which are acceptible
         * @throws NullPointerException if the given validValues is null
         */
        private ConstrainedSetValidator(final Collection<AllowableValue> validValues) {
            String validVals = "";
            if (!validValues.isEmpty()) {
                final StringBuilder valuesBuilder = new StringBuilder();
                for (final AllowableValue value : validValues) {
                    valuesBuilder.append(value).append(VALUE_DEMARCATOR);
                }
                validVals = valuesBuilder.substring(0, valuesBuilder.length() - VALUE_DEMARCATOR.length());
            }
            validStrings = validVals;

            this.validValues = new ArrayList<>(validValues.size());
            for (final AllowableValue value : validValues) {
                this.validValues.add(value.getValue());
            }
        }

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            builder.input(input);
            builder.subject(subject);
            if (validValues.contains(input)) {
                builder.valid(true);
                builder.explanation(POSITIVE_EXPLANATION);
            } else {
                builder.valid(false);
                builder.explanation(String.format(NEGATIVE_EXPLANATION, validStrings));
            }
            return builder.build();
        }
    }

    private static class ResourceDefinitionValidator implements Validator {
        private final ResourceDefinition resourceDefinition;
        private final ExpressionLanguageScope expressionLanguageScope;

        public ResourceDefinitionValidator(final ResourceDefinition resourceDefinition, final ExpressionLanguageScope expressionLanguageScope) {
            this.resourceDefinition = resourceDefinition;
            this.expressionLanguageScope = expressionLanguageScope;
        }

        @Override
        public ValidationResult validate(final String subject, final String configuredInput, final ValidationContext context) {
            final ValidationResult.Builder resultBuilder = new ValidationResult.Builder()
                .input(configuredInput)
                .subject(subject);

            if (configuredInput == null) {
                return resultBuilder.valid(false)
                    .explanation("No value specified")
                    .build();
            }

            // If Expression Language is supported and is used in the property value, we cannot perform validation against the configured
            // input unless the Expression Language is expressly limited to only variable registry. In that case, we can evaluate it and then
            // validate the value after evaluating the Expression Language.
            String input = configuredInput;
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(configuredInput)) {
                if (expressionLanguageScope != null && expressionLanguageScope == ExpressionLanguageScope.VARIABLE_REGISTRY) {
                    input = context.newPropertyValue(configuredInput).evaluateAttributeExpressions().getValue();
                    resultBuilder.input(input);
                } else {
                    return resultBuilder.valid(true)
                        .explanation("Expression Language is present, so validation of property value cannot be performed")
                        .build();
                }
            }

            // If the property can be text, then there's nothing to validate. Anything that is entered may be valid.
            // This will be improved in the future, by allowing the user to specify the type of resource that is being referenced.
            // Until then, we will simply require that the component perform any necessary validation.
            final boolean allowsText = resourceDefinition.getResourceTypes().contains(ResourceType.TEXT);
            if (allowsText) {
                return resultBuilder.valid(true)
                    .explanation("Property allows for Resource Type of Text, so validation of property value cannot be performed")
                    .build();
            }

            final String[] splits = input.split(",");
            if (resourceDefinition.getCardinality() == ResourceCardinality.SINGLE && splits.length > 1) {
                return resultBuilder.valid(false)
                    .explanation("Property only supports a single Resource but " + splits.length + " resources were specified")
                    .build();
            }

            final Set<ResourceType> resourceTypes = resourceDefinition.getResourceTypes();
            final List<String> nonExistentResources = new ArrayList<>();

            int count = 0;
            for (final String split : splits) {
                final ResourceReference resourceReference = new StandardResourceReferenceFactory().createResourceReference(split, resourceDefinition);
                if (resourceReference == null) {
                    continue;
                }

                count++;

                final boolean accessible = resourceReference.isAccessible();
                if (!accessible) {
                    nonExistentResources.add(resourceReference.getLocation());
                    continue;
                }

                if (!resourceTypes.contains(resourceReference.getResourceType())) {
                    return resultBuilder.valid(false)
                        .explanation("Specified Resource is a " + resourceReference.getResourceType().name() + " but this property does not allow this type of resource")
                        .build();
                }
            }

            if (count == 0) {
                return resultBuilder.valid(false)
                    .explanation("No resources were specified")
                    .build();
            }

            if (!nonExistentResources.isEmpty()) {
                return resultBuilder.valid(false)
                    .explanation("The specified resource(s) do not exist or could not be accessed: " + nonExistentResources)
                    .build();
            }

            return resultBuilder.valid(true)
                .build();
        }
    }
}
