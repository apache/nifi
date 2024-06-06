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
package org.apache.nifi.web.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

public class PaginationHelper {
    public static <T, E> List<T> paginateByContainedItems(
            final Iterable<T> original,
            final int offset,
            final int limit,
            final Function<T, List<E>> getContainedItems,
            final BiFunction<T, List<E>, T> createPartialItem
    ) {
        Objects.requireNonNull(original);
        Objects.requireNonNull(getContainedItems);
        Objects.requireNonNull(createPartialItem);

        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }

        if (limit < 0) {
            throw new IllegalArgumentException("Limit cannot be negative");
        }

        final List<T> result = new LinkedList<>();
        final int higherBoundary = limit == 0 ? 0 : offset + limit;
        final Interval interval = IntervalFactory.getClosedOpenInterval(offset, higherBoundary);
        int pointer = 0;

        if (offset == 0 && limit == 0) {
            original.forEach(result::add);
            return result;
        }

        for (final T candidate : original) {
            final List<E> containedItems = getContainedItems.apply(candidate);
            final ClosedOpenInterval.RelativePosition position = interval.getRelativePositionOf(pointer, pointer + containedItems.size());

            switch (position) {
                case BEFORE: {
                    pointer += containedItems.size();
                    break;
                }
                case EXCEEDS: {
                    final int startingPoint = offset - pointer;
                    final List<E> partialItems = containedItems.subList(startingPoint, limit + 1);
                    final T partial = createPartialItem.apply(candidate, partialItems);
                    result.add(partial);
                    pointer += startingPoint + partialItems.size();
                    break;
                }
                case TAIL_INTERSECTS: {
                    final List<E> partialItems = containedItems.subList(offset - pointer, containedItems.size());
                    final T partial = createPartialItem.apply(candidate, partialItems);
                    result.add(partial);
                    pointer += containedItems.size();
                    break;
                }
                case WITHIN: {
                    result.add(candidate);
                    pointer += containedItems.size();
                    break;
                }
                case HEAD_INTERSECTS: {
                    final List<E> partialItems = containedItems.subList(0, limit + offset - pointer);
                    final T partial = createPartialItem.apply(candidate, partialItems);
                    result.add(partial);
                    pointer += partialItems.size();
                    break;
                }
                case AFTER:
                default:
                    // Do nothing
            }
        }

        return result;
    }
}
