package org.apache.nifi.nio;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;

public interface ChainedAction extends MessageSequence {

    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface InitialAction {
    }

    @Override
    default void clear() {
        MessageSequence.super.clear();
        init();
    }

    default void init() {

        Field initialAction = null;
        for (Field field : getClass().getDeclaredFields()) {
            if (MessageAction.class.isAssignableFrom(field.getType())
                && field.isAnnotationPresent(InitialAction.class)) {
                initialAction = field;
                break;
            }
        }

        final Field nextAction;
        try {
            nextAction = getClass().getDeclaredField("nextAction");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("ChainedAction implementation class must declare MessageAction field named 'nextAction'");
        }

        if (initialAction == null) {
            throw new RuntimeException("No MessageAction field with @InitialAction was found in order to initialize 'nextAction'");
        }

        try {
            initialAction.setAccessible(true);
            nextAction.setAccessible(true);
            nextAction.set(this, initialAction.get(this));
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    default boolean shouldAutoClear(Field field) {
        // A field with @NextAction should be initialized via init().
        return MessageSequence.super.shouldAutoClear(field) && !"nextAction".equals(field.getName());
    }
}
