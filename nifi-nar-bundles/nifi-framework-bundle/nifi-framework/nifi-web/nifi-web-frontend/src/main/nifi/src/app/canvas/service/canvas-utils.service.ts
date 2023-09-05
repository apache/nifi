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

import { Injectable } from '@angular/core';
import * as d3 from 'd3';
import { select, Store } from '@ngrx/store';
import { CanvasState } from '../state';
import { selectCurrentProcessGroupId } from '../state/flow/flow.selectors';

@Injectable({
    providedIn: 'root'
})
export class CanvasUtils {
    private trimLengthCaches: Map<string, Map<string, Map<number, number>>> = new Map();

    constructor(private store: Store<CanvasState>) {
        this.store.pipe(select(selectCurrentProcessGroupId)).subscribe(() => {
            this.trimLengthCaches.clear();
        });
    }

    public canModify(selection: any): boolean {
        var selectionSize = selection.size();
        var writableSize = selection
            .filter(function (d: any) {
                return d.permissions.canWrite;
            })
            .size();

        return selectionSize === writableSize;
    }

    /**
     * Extracts the contents of the specified str before the strToFind. If the
     * strToFind is not found or the first path of the str, an empty string is
     * returned.
     *
     * @argument {string} str       The full string
     * @argument {string} strToFind The substring to find
     */
    public substringBeforeFirst(str: string, strToFind: string) {
        let result = '';
        const indexOfStrToFind = str.indexOf(strToFind);
        if (indexOfStrToFind >= 0) {
            result = str.substring(0, indexOfStrToFind);
        }
        return result;
    }

    /**
     * Extracts the contents of the specified str after the strToFind. If the
     * strToFind is not found or the last part of the str, an empty string is
     * returned.
     *
     * @argument {string} str       The full string
     * @argument {string} strToFind The substring to find
     */
    public substringAfterFirst(str: string, strToFind: string) {
        var result = '';
        var indexOfStrToFind = str.indexOf(strToFind);
        if (indexOfStrToFind >= 0) {
            var indexAfterStrToFind = indexOfStrToFind + strToFind.length;
            if (indexAfterStrToFind < str.length) {
                result = str.substring(indexAfterStrToFind);
            }
        }
        return result;
    }

    /**
     * Extracts the contents of the specified str after the last strToFind. If the
     * strToFind is not found or the last part of the str, an empty string is
     * returned.
     *
     * @argument {string} str       The full string
     * @argument {string} strToFind The substring to find
     */
    public substringAfterLast(str: string, strToFind: string): string {
        let result = '';
        const indexOfStrToFind = str.lastIndexOf(strToFind);
        if (indexOfStrToFind >= 0) {
            const indexAfterStrToFind = indexOfStrToFind + strToFind.length;
            if (indexAfterStrToFind < str.length) {
                result = str.substring(indexAfterStrToFind);
            }
        }
        return result;
    }

    /**
     * Determines whether the specified string is blank (or null or undefined).
     *
     * @argument {string} str   The string to test
     */
    public isBlank(str: string) {
        if (str) {
            return str.trim().length > 0;
        }

        return true;
    }

    /**
     * Determines if the specified array is empty. If the specified arg is not an
     * array, then true is returned.
     *
     * @argument {array} arr    The array to test
     */
    public isEmpty(arr: any) {
        return Array.isArray(arr) ? arr.length === 0 : true;
    }

    /**
     * Formats the class name of this component.
     *
     * @param dataContext component datum
     */
    public formatClassName(dataContext: any): string {
        return this.substringAfterLast(dataContext.type, '.');
    }

    /**
     * Formats the type of this component.
     *
     * @param dataContext component datum
     */
    public formatType(dataContext: any): string {
        let typeString: string = this.formatClassName(dataContext);
        if (dataContext.bundle.version !== 'unversioned') {
            typeString += ' ' + dataContext.bundle.version;
        }
        return typeString;
    }

    /**
     * Formats the bundle label.
     *
     * @param bundle
     */
    public formatBundle(bundle: any): string {
        let groupString: string = '';
        if (bundle.group !== 'default') {
            groupString = bundle.group + ' - ';
        }
        return groupString + bundle.artifact;
    }

    private binarySearch(length: number, comparator: Function): number {
        let low = 0;
        let high = length - 1;
        let mid = 0;

        let result = 0;
        while (low <= high) {
            mid = ~~((low + high) / 2);
            result = comparator(mid);
            if (result < 0) {
                high = mid - 1;
            } else if (result > 0) {
                low = mid + 1;
            } else {
                break;
            }
        }

        return mid;
    }

    /**
     * Applies single line ellipsis to the component in the specified selection if necessary.
     *
     * @param {selection} selection
     * @param {string} text
     * @param {string} cacheName
     */
    public ellipsis(selection: any, text: string, cacheName: string) {
        text = text.trim();
        let width = parseInt(selection.attr('width'), 10);
        const node = selection.node();

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
        if (!trimLength) {
            // We haven't cached the length for this text yet. Determine whether we need
            // to trim & add ellipses or not
            if (node.getSubStringLength(0, text.length - 1) > width) {
                // make some room for the ellipsis
                width -= 5;

                // determine the appropriate index
                trimLength = this.binarySearch(text.length, function (x: number) {
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

    /**
     * Applies multiline ellipsis to the component in the specified seleciton. Text will
     * wrap for the specified number of lines. The last line will be ellipsis if necessary.
     *
     * @param {selection} selection
     * @param {number} lineCount
     * @param {string} text
     * @param {string} cacheName
     */
    private multilineEllipsis(selection: any, lineCount: number, text: string, cacheName: string) {
        let i: number = 1;
        const words: string[] = text.split(/\s+/).reverse();

        // get the appropriate position
        const x = parseInt(selection.attr('x'), 10);
        const y = parseInt(selection.attr('y'), 10);
        const width = parseInt(selection.attr('width'), 10);

        let line: string[] = [];
        let tspan = selection.append('tspan').attr('x', x).attr('y', y).attr('width', width);

        // go through each word
        let word = words.pop();
        while (!!word) {
            // add the current word
            line.push(word);

            // update the label text
            tspan.text(line.join(' '));

            // if this word caused us to go too far
            if (tspan.node().getComputedTextLength() > width) {
                // remove the current word
                line.pop();

                // update the label text
                tspan.text(line.join(' '));

                // create the tspan for the next line
                tspan = selection.append('tspan').attr('x', x).attr('dy', '1.2em').attr('width', width);

                // if we've reached the last line, use single line ellipsis
                if (++i >= lineCount) {
                    // get the remainder using the current word and
                    // reversing whats left
                    var remainder = [word].concat(words.reverse());

                    // apply ellipsis to the last line
                    this.ellipsis(tspan, remainder.join(' '), cacheName);

                    // we've reached the line count
                    break;
                } else {
                    tspan.text(word);

                    // prep the line for the next iteration
                    line = [word];
                }
            }

            // get the next word
            word = words.pop();
        }
    }

    /**
     * Updates the active thread count on the specified selection.
     *
     * @param {selection} selection         The selection
     * @param {object} d                    The data
     * @param {function} setOffset          Optional function to handle the width of the active thread count component
     * @return
     */
    public activeThreadCount(selection: any, d: any, setOffset: Function) {
        const activeThreads = d.status.aggregateSnapshot.activeThreadCount;
        const terminatedThreads = d.status.aggregateSnapshot.terminatedThreadCount;

        // if there is active threads show the count, otherwise hide
        if (activeThreads > 0 || terminatedThreads > 0) {
            const generateThreadsTip = function () {
                var tip = activeThreads + ' active threads';
                if (terminatedThreads > 0) {
                    tip += ' (' + terminatedThreads + ' terminated)';
                }

                return tip;
            };

            // update the active thread count
            const activeThreadCount = selection
                .select('text.active-thread-count')
                .text(function () {
                    if (terminatedThreads > 0) {
                        return activeThreads + ' (' + terminatedThreads + ')';
                    } else {
                        return activeThreads;
                    }
                })
                .style('display', 'block')
                .each(function (this: any) {
                    const activeThreadCountText = d3.select(this);

                    const bBox = this.getBBox();
                    activeThreadCountText.attr('x', function () {
                        return d.dimensions.width - bBox.width - 15;
                    });

                    // reset the active thread count tooltip
                    activeThreadCountText.selectAll('title').remove();
                });

            // append the tooltip
            activeThreadCount.append('title').text(generateThreadsTip);

            // update the background width
            selection
                .select('text.active-thread-count-icon')
                .attr('x', function () {
                    const bBox = activeThreadCount.node().getBBox();

                    // update the offset
                    if (typeof setOffset === 'function') {
                        setOffset(bBox.width + 6);
                    }

                    return d.dimensions.width - bBox.width - 20;
                })
                .style('fill', function () {
                    if (terminatedThreads > 0) {
                        return '#ba554a';
                    } else {
                        return '#728e9b';
                    }
                })
                .style('display', 'block')
                .each(function (this: any) {
                    const activeThreadCountIcon = d3.select(this);

                    // reset the active thread count tooltip
                    activeThreadCountIcon.selectAll('title').remove();
                })
                .append('title')
                .text(generateThreadsTip);
        } else {
            selection
                .selectAll('text.active-thread-count, text.active-thread-count-icon')
                .style('display', 'none')
                .each(function (this: any) {
                    d3.select(this).selectAll('title').remove();
                });
        }
    }

    /**
     * Determines the contrast color of a given hex color.
     *
     * @param {string} hex  The hex color to test.
     * @returns {string} The contrasting color string.
     */
    public determineContrastColor(hex: string): string {
        if (parseInt(hex, 16) > 0xffffff / 1.5) {
            return '#000000';
        }
        return '#ffffff';
    }
}
