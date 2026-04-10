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

import * as d3 from 'd3';

export class TextEllipsisUtils {
    // Cache structure: Map<cacheName, Map<text, Map<width, trimLength>>>
    private trimLengthCaches = new Map<string, Map<string, Map<number, number>>>();

    public applyEllipsis(selection: d3.Selection<any, any, any, any>, text: string, cacheName: string): void {
        text = text.trim();
        let width = parseInt(selection.attr('width'), 10);
        const node = selection.node() as SVGTextElement;

        if (!node) {
            return;
        }

        // set the element text
        selection.text(text);

        // Never apply ellipses to text less than 5 characters and don't keep it in the cache
        // because it could take up a lot of space unnecessarily.
        const textLength: number = text.length;
        if (textLength < 5) {
            return;
        }

        // Check our cache of text lengths to see if we already know how much to trim it to
        let trimLengths = this.trimLengthCaches.get(cacheName);
        if (!trimLengths) {
            trimLengths = new Map();
            this.trimLengthCaches.set(cacheName, trimLengths);
        }

        const cacheForText = trimLengths.get(text);
        let trimLength = cacheForText === undefined ? undefined : cacheForText.get(width);
        if (trimLength === undefined) {
            // We haven't cached the length for this text yet. Determine whether we need
            // to trim & add ellipses or not
            const measuredLength = node.getSubStringLength(0, text.length - 1);

            if (measuredLength > width) {
                // calculate the ellipsis length since it varies greatly based on the font size
                selection.text(String.fromCharCode(8230));
                const ellipsisLength = node.getSubStringLength(0, 1);

                // restore the actual value
                selection.text(text);

                // make some room for the ellipsis
                width -= ellipsisLength * 1.5;

                // determine the appropriate index using binary search
                trimLength = TextEllipsisUtils.binarySearch(text.length, (x: number) => {
                    const length = node.getSubStringLength(0, x);
                    if (length > width) {
                        // length is too long, try the lower half
                        return -1;
                    } else if (length < width) {
                        // length is too short, try the upper half
                        return 1;
                    }
                    return 0;
                });
            } else {
                // trimLength of -1 indicates we do not need ellipses
                trimLength = -1;
            }

            // Store the trim length in our cache
            let trimLengthsForText = trimLengths.get(text);
            if (trimLengthsForText === undefined) {
                trimLengthsForText = new Map();
                trimLengths.set(text, trimLengthsForText);
            }
            trimLengthsForText.set(width, trimLength);
        }

        if (trimLength === -1) {
            return;
        }

        // trim at the appropriate length and add ellipsis
        selection.text(text.substring(0, trimLength) + String.fromCharCode(8230));
    }

    private static binarySearch(length: number, comparator: (x: number) => number): number {
        let low = 0;
        let high = length;
        let mid = 0;

        let result: number;
        while (low <= high) {
            // calculate the mid point
            mid = Math.floor((low + high) / 2);
            result = comparator(mid);

            // check the result
            if (result < 0) {
                // the index is too high
                high = mid - 1;
            } else if (result > 0) {
                // the index is too low
                low = mid + 1;
            } else {
                // we've found it!
                break;
            }
        }

        return mid;
    }

    public clearCache(): void {
        this.trimLengthCaches.clear();
    }

    public clearCacheForName(cacheName: string): void {
        this.trimLengthCaches.delete(cacheName);
    }

    public determineContrastColor(hexColor: string): string {
        // Remove # if present
        const hex = hexColor.replace('#', '');

        // Convert to RGB
        const r = parseInt(hex.substring(0, 2), 16);
        const g = parseInt(hex.substring(2, 4), 16);
        const b = parseInt(hex.substring(4, 6), 16);

        // Calculate luminance using standard formula
        // https://www.w3.org/TR/WCAG20/#relativeluminancedef
        const luminance = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;

        // Return black for light backgrounds, white for dark backgrounds
        return luminance > 0.5 ? '#000000' : '#ffffff';
    }
}
