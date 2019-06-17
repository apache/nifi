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

import org.junit.Assert;
import org.junit.Test;

public class TestProcessGroupBox {

    @Test
    public void testIntersectsWhenCompletelyAbove() {
        final ProcessGroupBox pg1 = new ProcessGroupBox(0, 0);
        final ProcessGroupBox pg2 = new ProcessGroupBox(0, ProcessGroupBox.PG_SIZE_HEIGHT * 2 + 10);
        Assert.assertFalse(pg1.intersects(pg2));
    }

    @Test
    public void testIntersectsWhenCompletelyBelow() {
        final ProcessGroupBox pg1 = new ProcessGroupBox(0, ProcessGroupBox.PG_SIZE_HEIGHT * 2 + 10);
        final ProcessGroupBox pg2 = new ProcessGroupBox(0, 0);
        Assert.assertFalse(pg1.intersects(pg2));
    }

    @Test
    public void testIntersectsWhenCompletelyLeft() {
        final ProcessGroupBox pg1 = new ProcessGroupBox(0, 0);
        final ProcessGroupBox pg2 = new ProcessGroupBox(ProcessGroupBox.PG_SIZE_WIDTH * 2 + 10,0);
        Assert.assertFalse(pg1.intersects(pg2));
    }

    @Test
    public void testIntersectsWhenCompletelyRight() {
        final ProcessGroupBox pg1 = new ProcessGroupBox(ProcessGroupBox.PG_SIZE_WIDTH * 2 + 10,0);
        final ProcessGroupBox pg2 = new ProcessGroupBox(0,0);
        Assert.assertFalse(pg1.intersects(pg2));
    }

    @Test
    public void testIntersectsWhenCompletelyOverlapping() {
        final ProcessGroupBox pg1 = new ProcessGroupBox(0,0);
        final ProcessGroupBox pg2 = new ProcessGroupBox(0,0);
        Assert.assertTrue(pg1.intersects(pg2));
    }

    @Test
    public void testIntersectsWhenPartiallyOverlappingVertically() {
        final ProcessGroupBox pg1 = new ProcessGroupBox(0,0);
        final ProcessGroupBox pg2 = new ProcessGroupBox(0,ProcessGroupBox.PG_SIZE_HEIGHT / 2);
        Assert.assertTrue(pg1.intersects(pg2));
    }

    @Test
    public void testIntersectsWhenBottomAndTopSame() {
        final ProcessGroupBox pg1 = new ProcessGroupBox(0,0);
        final ProcessGroupBox pg2 = new ProcessGroupBox(0,ProcessGroupBox.PG_SIZE_HEIGHT);
        Assert.assertTrue(pg1.intersects(pg2));
    }

    @Test
    public void testIntersectsWhenPartiallyOverlappingHorizontally() {
        final ProcessGroupBox pg1 = new ProcessGroupBox(0,0);
        final ProcessGroupBox pg2 = new ProcessGroupBox(ProcessGroupBox.PG_SIZE_WIDTH / 2,0);
        Assert.assertTrue(pg1.intersects(pg2));
    }

    @Test
    public void testIntersectsWhenRightAndLeftSame() {
        final ProcessGroupBox pg1 = new ProcessGroupBox(0,0);
        final ProcessGroupBox pg2 = new ProcessGroupBox(ProcessGroupBox.PG_SIZE_WIDTH,0);
        Assert.assertTrue(pg1.intersects(pg2));
    }
}
