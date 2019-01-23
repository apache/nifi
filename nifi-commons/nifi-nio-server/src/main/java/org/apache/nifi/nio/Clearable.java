package org.apache.nifi.nio;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface Clearable {

    /**
     * <p>Clears internal state.</p>
     *
     * <p>This method is called from a selected key processing loop when this sequence is done.
     * So that the same instance can accept a new sequence from beginning.
     * The same instance is kept being attached to a client channel, and accept multiple protocol sequences.</p>
     *
     * <p>By default, all {@link Clearable} fields or any field annotated with {@link AutoCleared} is cleared.
     * If such field is:
     * <li>{@link Clearable}, its {@link Clearable#clear()} is called</li>
     * <li>primitive boolean, reset to false</li>
     * <li>primitive short/int/long, reset to 0</li>
     * <li>otherwise null cleared</li>
     * </p>
     */
    default void clear() {
        final List<Field> fields = new ArrayList<>();
        Class<?> clazz = getClass();
        while (Clearable.class.isAssignableFrom(clazz)) {
            fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
            clazz = clazz.getSuperclass();
        }

        for (Field field : fields) {
            if (shouldAutoClear(field)) {
                try {
                    field.setAccessible(true);
                    final Class<?> type = field.getType();
                    if (Clearable.class.isAssignableFrom(type)) {
                        final Clearable clearable = (Clearable) field.get(this);
                        if (clearable != null) {
                            clearable.clear();
                        }
                    } else if (type.equals(boolean.class)) {
                        field.set(this, false);
                    } else if (type.equals(short.class)) {
                        field.set(this, 0);
                    } else if (type.equals(int.class)) {
                        field.set(this, 0);
                    } else if (type.equals(long.class)) {
                        field.set(this, 0);
                    } else {
                        field.set(this, null);
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    default boolean shouldAutoClear(Field field) {
        return Clearable.class.isAssignableFrom(field.getType())
            || field.isAnnotationPresent(AutoCleared.class);
    }

}
