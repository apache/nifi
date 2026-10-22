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

/**
 * Minimum scale at which canvas components render their detail subtrees.
 * Below this threshold every component is classified as not-visible regardless
 * of its position, so entering/leaving events don't fire during a deep zoom-out.
 */
export const MIN_SCALE_TO_RENDER = 0.4;

/**
 * Axis-aligned bounding rectangle in canvas coordinate space.
 * The canvas publishes this as a signal; each layer renderer consumes it to
 * determine per-component visibility.
 */
export interface ViewportRect {
    left: number;
    top: number;
    right: number;
    bottom: number;
}

/**
 * Determine whether a canvas component is inside the visible viewport.
 *
 * The viewport rectangle is the padded 3× screen area (one screen of padding
 * in every direction) so components pre-render slightly before they scroll into
 * view. Components are classified as not-visible when scale is below
 * MIN_SCALE_TO_RENDER so that a deep zoom-out does not trigger a mass
 * entering/leaving cascade.
 *
 * @param position   - Top-left canvas-space position of the component
 * @param dimensions - Width/height of the component bounding box
 * @param viewport   - Current padded viewport rectangle (null → not measurable)
 * @param scale      - Current canvas scale factor
 */
export function isInViewport(
    position: { x: number; y: number },
    dimensions: { width: number; height: number },
    viewport: ViewportRect | null,
    scale: number
): boolean {
    if (!viewport || scale < MIN_SCALE_TO_RENDER) {
        return false;
    }
    const left = position.x;
    const top = position.y;
    const right = left + dimensions.width;
    const bottom = top + dimensions.height;
    return !(right < viewport.left || left > viewport.right || bottom < viewport.top || top > viewport.bottom);
}
