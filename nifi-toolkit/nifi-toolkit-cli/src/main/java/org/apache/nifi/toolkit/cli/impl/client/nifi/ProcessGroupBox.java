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
package org.apache.nifi.toolkit.cli.impl.client.nifi;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents the bounding box the Processing Group and some placement logic.
 */
public class ProcessGroupBox implements Comparable<ProcessGroupBox> {
    // values as specified in the nf-process-group.js file
    public static final int PG_SIZE_WIDTH = 380;
    public static final int PG_SIZE_HEIGHT = 172;
    // minimum whitespace between PG elements for auto-layout
    public static final int PG_SPACING = 50;

    private final int x;
    private final int y;

    public static final ProcessGroupBox CANVAS_CENTER = new ProcessGroupBox(0, 0);

    public ProcessGroupBox(int x, int y) {
        this.x = x;
        this.y = y;
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    /**
     * @return distance from a (0, 0) point.
     */
    public int distance() {
        // a simplified distance formula because the other coord is (0, 0)
        return (int) Math.hypot(x, y);
    }


    public boolean intersects(ProcessGroupBox other) {
        // adapted from java.awt Rectangle, we don't want to import it
        // assume everything to be of the PG size for simplicity
        int tw = PG_SIZE_WIDTH;
        int th = PG_SIZE_HEIGHT;
        // 2nd pg box includes spacers
        int rw = PG_SIZE_WIDTH;
        int rh = PG_SIZE_HEIGHT;
        if (rw <= 0 || rh <= 0 || tw <= 0 || th <= 0) {
            return false;
        }
        double tx = this.x;
        double ty = this.y;
        double rx = other.x;
        double ry = other.y;
        rw += rx;
        rh += ry;
        tw += tx;
        th += ty;
        //      overflow || intersect
        return ((rw < rx || rw > tx)
                && (rh < ry || rh > ty)
                && (tw < tx || tw > rx)
                && (th < ty || th > ry));
    }


    public ProcessGroupBox findFreeSpace(List<ProcessGroupBox> allCoords) {
        // sort by distance to (0.0)
        List<ProcessGroupBox> byClosest = allCoords.stream().sorted().collect(Collectors.toList());

        // search to the right
        List<ProcessGroupBox> freeSpots = byClosest.stream().filter(other ->
                byClosest.stream().noneMatch(other.right()::intersects)
        ).map(ProcessGroupBox::right).collect(Collectors.toList()); // save a 'transformed' spot 'to the right'

        // search down
        freeSpots.addAll(byClosest.stream().filter(other ->
                byClosest.stream().noneMatch(other.down()::intersects)
        ).map(ProcessGroupBox::down).collect(Collectors.toList()));

        // search left
        freeSpots.addAll(byClosest.stream().filter(other ->
                byClosest.stream().noneMatch(other.left()::intersects)
        ).map(ProcessGroupBox::left).collect(Collectors.toList()));

        // search above
        freeSpots.addAll(byClosest.stream().filter(other ->
                byClosest.stream().noneMatch(other.up()::intersects)
        ).map(ProcessGroupBox::up).collect(Collectors.toList()));

        // return a free spot closest to (0, 0)
        return freeSpots.stream().sorted().findFirst().orElse(CANVAS_CENTER);
    }

    public ProcessGroupBox right() {
        return new ProcessGroupBox(this.x + PG_SIZE_WIDTH + PG_SPACING, this.y);
    }

    public ProcessGroupBox down() {
        return new ProcessGroupBox(this.x, this.y + PG_SIZE_HEIGHT + PG_SPACING);
    }

    public ProcessGroupBox up() {
        return new ProcessGroupBox(this.x, this.y - PG_SPACING - PG_SIZE_HEIGHT);
    }

    public ProcessGroupBox left() {
        return new ProcessGroupBox(this.x - PG_SPACING - PG_SIZE_WIDTH, this.y);
    }

    @Override
    public int compareTo(ProcessGroupBox other) {
        return this.distance() - other.distance();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProcessGroupBox pgBox = (ProcessGroupBox) o;
        return Double.compare(pgBox.x, x) == 0
                && Double.compare(pgBox.y, y) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }
}
