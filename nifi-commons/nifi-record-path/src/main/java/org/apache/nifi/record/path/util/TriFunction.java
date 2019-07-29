package org.apache.nifi.record.path.util;

@FunctionalInterface
public interface TriFunction<One, Two, Three, Result> {
    Result apply(One one, Two two, Three three);
}
