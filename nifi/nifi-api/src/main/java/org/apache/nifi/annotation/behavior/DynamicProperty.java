package org.apache.nifi.annotation.behavior;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.nifi.components.ConfigurableComponent;

/**
 * An annotation that may be placed on a {@link ConfigurableComponent} to
 * indicate that it supports a dynamic property.
 * 
 * @author
 *
 */
@Documented
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface DynamicProperty {
    /**
     * A description of what the name of the dynamic property may be
     * 
     * @return A description of what the name of the dynamic property may be
     */
    public String name();

    /**
     * Indicates whether or not the dynamic property supports expression
     * language
     * 
     * @return whether or not the dynamic property supports expression
     *         language
     */
    public boolean supportsExpressionLanguage() default false;
    
    /**
     * A description of what the value of the dynamic property may be
     * @return a description of what the value of the dynamic property may be
     */
    public String value();
    
    /**
     * Provides a description of what the meaning of the property is, and what the expected values are 
     * @return a description of what the meaning of the property is, and what the expected values are
     */
    public String description();
}
