package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents the bounding box the Processing Group and some placement logic.
 */
public class PgBox implements Comparable<PgBox> {
    // values as specified in the nf-process-group.js file
    public static final int PG_SIZE_WIDTH = 380;
    public static final int PG_SIZE_HEIGHT = 172;
    // minimum whitespace between PG elements for auto-layout
    public static final int PG_SPACING = 50;
    public int x;
    public int y;

    public static final PgBox CANVAS_CENTER = new PgBox(0, 0);

    public PgBox(int x, int y) {
        this.x = x;
        this.y = y;
    }

    /**
     * @return distance from a (0, 0) point.
     */
    public int distance() {
        // a simplified distance formula because the other coord is (0, 0)
        return (int) Math.hypot(x, y);
    }


    public boolean intersects(PgBox other) {
        // adapted for java.awt Rectangle, we don't want to import it
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
        return ((rw < rx || rw > tx) &&
                (rh < ry || rh > ty) &&
                (tw < tx || tw > rx) &&
                (th < ty || th > ry));
    }


    public PgBox findFreeSpace(List<PgBox> allCoords) {
        // sort by distance to (0.0)
        List<PgBox> byClosest = allCoords.stream().sorted().collect(Collectors.toList());

        // search to the right
        List<PgBox> freeSpots = byClosest.stream().filter(other ->
                byClosest.stream().noneMatch(other.right()::intersects)
        ).map(PgBox::right).collect(Collectors.toList()); // save a 'transformed' spot 'to the right'

        // search down
        freeSpots.addAll(byClosest.stream().filter(other ->
                byClosest.stream().noneMatch(other.down()::intersects)
        ).map(PgBox::down).collect(Collectors.toList()));

        // search left
        freeSpots.addAll(byClosest.stream().filter(other ->
                byClosest.stream().noneMatch(other.left()::intersects)
        ).map(PgBox::left).collect(Collectors.toList()));

        // search above
        freeSpots.addAll(byClosest.stream().filter(other ->
                byClosest.stream().noneMatch(other.up()::intersects)
        ).map(PgBox::up).collect(Collectors.toList()));

        // return a free spot closest to (0, 0)
        return freeSpots.stream().sorted().findFirst().orElse(CANVAS_CENTER);
    }

    public PgBox right() {
        return new PgBox(this.x + PG_SIZE_WIDTH + PG_SPACING, this.y);
    }

    public PgBox down() {
        return new PgBox(this.x, this.y + PG_SIZE_HEIGHT + PG_SPACING);
    }

    public PgBox up() {
        return new PgBox(this.x, this.y - PG_SPACING - PG_SIZE_HEIGHT);
    }

    public PgBox left() {
        return new PgBox(this.x - PG_SPACING - PG_SIZE_WIDTH, this.y);
    }

    @Override
    public int compareTo(PgBox other) {
        return this.distance() - other.distance();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PgBox pgBox = (PgBox) o;
        return Double.compare(pgBox.x, x) == 0 &&
                Double.compare(pgBox.y, y) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }
}
