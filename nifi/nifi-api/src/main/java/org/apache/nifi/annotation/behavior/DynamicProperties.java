package org.apache.nifi.annotation.behavior;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a component has more than one dynamic property
 * 
 * @author
 *
 */
@Documented
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface DynamicProperties {
    /**
     * A list of the dynamic properties supported by a component
     * @return A list of the dynamic properties supported by a component
     */
    public DynamicProperty[] value();
}
