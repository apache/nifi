package org.apache.nifi.annotation.documentation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;

/**
 * Annotation to indicate that a {@link Processor} supports dynamic
 * relationship. A dynamic {@link Relationship} is one where the relationship is
 * generated based on a user defined {@link PropertyDescriptor}
 * 
 * @author
 *
 */
@Documented
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface DynamicRelationships {
    /**
     * Describes the name(s) of the dynamic relationship(s)
     * 
     * @return a description of the name(s) of the dynamic relationship(s)
     */
    public String name();

    /**
     * Describes the data that should be routed to the dynamic relationship(s)
     * 
     * @return a description the data that should be routed to the dynamic relationship(s)
     */
    public String description();
}
