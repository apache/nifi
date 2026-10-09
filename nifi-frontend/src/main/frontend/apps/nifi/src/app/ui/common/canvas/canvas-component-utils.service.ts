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

import { Injectable, inject, Type } from '@angular/core';
import * as d3 from 'd3';
import { BulletinEntity, ComponentType, NiFiCommon, TextTip } from '@nifi/shared';
import { Overlay, OverlayRef, PositionStrategy } from '@angular/cdk/overlay';
import { ComponentPortal } from '@angular/cdk/portal';
import { BulletinsTip } from '../tooltips/bulletins-tip/bulletins-tip.component';
import { humanizer, Humanizer } from 'humanize-duration';
import { ActiveThreadCountDatum } from './canvas.types';

@Injectable({
    providedIn: 'root'
})
export class CanvasComponentUtils {
    private nifiCommon = inject(NiFiCommon);
    private overlay = inject(Overlay);
    private readonly humanizeDuration: Humanizer = humanizer();

    public activeThreadCount<GElement extends d3.BaseType, Datum, PElement extends d3.BaseType, PDatum>(
        selection: d3.Selection<GElement, Datum, PElement, PDatum>,
        d: ActiveThreadCountDatum
    ): void {
        const aggregateSnapshot =
            'status' in d.entity && d.entity.status && 'aggregateSnapshot' in d.entity.status
                ? d.entity.status.aggregateSnapshot
                : undefined;
        const activeThreads =
            aggregateSnapshot && 'activeThreadCount' in aggregateSnapshot
                ? aggregateSnapshot.activeThreadCount || 0
                : 0;
        const terminatedThreads =
            aggregateSnapshot && 'terminatedThreadCount' in aggregateSnapshot
                ? aggregateSnapshot.terminatedThreadCount || 0
                : 0;

        // if there is active threads show the count, otherwise hide
        if (activeThreads > 0 || terminatedThreads > 0) {
            const generateThreadsTip = function () {
                let tip = activeThreads + ' active threads';
                if (terminatedThreads > 0) {
                    tip += ' (' + terminatedThreads + ' terminated)';
                }
                return tip;
            };

            // update the active thread count
            const activeThreadCount = selection
                .select<SVGTextElement>('text.active-thread-count')
                .text(function () {
                    if (terminatedThreads > 0) {
                        return activeThreads + ' (' + terminatedThreads + ')';
                    } else {
                        return activeThreads;
                    }
                })
                .attr('class', function () {
                    switch (d.ui.componentType) {
                        case ComponentType.Processor:
                        case ComponentType.InputPort:
                        case ComponentType.OutputPort:
                            return `active-thread-count tertiary-color`;
                        default:
                            return `active-thread-count secondary-contrast`;
                    }
                })
                .style('display', 'block')
                .each(function (this: SVGTextElement) {
                    const activeThreadCountText = d3.select(this);

                    const bBox = this.getBBox();
                    activeThreadCountText.attr('x', function () {
                        const width = 'dimensions' in d.ui ? d.ui.dimensions.width : 0;
                        return width - bBox.width - 15;
                    });

                    // reset the active thread count tooltip
                    activeThreadCountText.selectAll('title').remove();
                });

            // append the tooltip
            activeThreadCount.append('title').text(generateThreadsTip);

            // update the background width
            selection
                .select<SVGTextElement>('text.active-thread-count-icon')
                .attr('x', function () {
                    const node = activeThreadCount.node();
                    const bBox = node ? node.getBBox() : { width: 0 };
                    const width = 'dimensions' in d.ui ? d.ui.dimensions.width : 0;
                    return width - bBox.width - 20;
                })
                .attr('class', function () {
                    switch (d.ui.componentType) {
                        case ComponentType.Processor:
                        case ComponentType.InputPort:
                        case ComponentType.OutputPort:
                            if (terminatedThreads > 0) {
                                return `active-thread-count-icon error-color`;
                            } else {
                                return `active-thread-count-icon primary-color`;
                            }
                        default:
                            return `active-thread-count-icon secondary-contrast`;
                    }
                })
                .style('display', 'block')
                .each(function (this: SVGTextElement) {
                    const activeThreadCountIcon = d3.select(this);

                    // reset the active thread count tooltip
                    activeThreadCountIcon.selectAll('title').remove();
                })
                .append('title')
                .text(generateThreadsTip);
        } else {
            selection
                .selectAll<SVGTextElement, Datum>('text.active-thread-count, text.active-thread-count-icon')
                .style('display', 'none')
                .each(function (this: SVGTextElement) {
                    d3.select(this).selectAll('title').remove();
                });
        }
    }

    public bulletins<GElement extends d3.BaseType, Datum, PElement extends d3.BaseType, PDatum>(
        selection: d3.Selection<GElement, Datum, PElement, PDatum>,
        bulletins: BulletinEntity[] | undefined
    ): void {
        let filteredBulletins: BulletinEntity[] = [];
        if (bulletins) {
            filteredBulletins = bulletins.filter((bulletin) => bulletin.canRead && bulletin.bulletin);
        }

        if (this.nifiCommon.isEmpty(filteredBulletins)) {
            this.resetBulletin(selection);
        } else {
            // determine the most severe of the bulletins
            const mostSevere = this.nifiCommon.getMostSevereBulletin(filteredBulletins);

            // add the proper class to indicate the most severe bulletin
            if (mostSevere) {
                // Get references to bulletin elements
                const bulletinIcon = selection.select<SVGTextElement>('text.bulletin-icon');
                const bulletinBackground = selection.select<SVGRectElement>('rect.bulletin-background');

                // show the bulletin icon/background
                bulletinIcon.style('visibility', 'visible');
                bulletinBackground.style('visibility', 'visible');

                // reset any level-specifying classes that might have been there before
                bulletinIcon
                    .classed('trace', false)
                    .classed('debug', false)
                    .classed('info', false)
                    .classed('warning', false)
                    .classed('error', false);
                bulletinBackground
                    .classed('trace', false)
                    .classed('debug', false)
                    .classed('info', false)
                    .classed('warning', false)
                    .classed('error', false);

                bulletinIcon.classed(mostSevere.bulletin.level.toLowerCase(), true);
                bulletinBackground.classed(mostSevere.bulletin.level.toLowerCase(), true);

                // add the tooltip
                this.canvasTooltip(BulletinsTip, bulletinIcon, {
                    bulletins: filteredBulletins
                });
            } else {
                // This could be a case where the component producing the previous bulletins has been deleted.
                // There is no bulletin to display if there is not a most severe, so hide the bulletin icon.
                this.resetBulletin(selection);
            }
        }
    }

    public comments<GElement extends d3.BaseType, Datum, PElement extends d3.BaseType, PDatum>(
        selection: d3.Selection<GElement, Datum, PElement, PDatum>,
        comments: string | null | undefined
    ): void {
        const commentIcon = selection.select<SVGTextElement>('text.component-comments');
        const hasComments = comments && comments.trim().length > 0;

        // Update visibility
        commentIcon.style('visibility', hasComments ? 'visible' : 'hidden');

        // Update tooltip
        if (hasComments) {
            this.canvasTooltip(TextTip, commentIcon, comments);
        } else {
            this.resetCanvasTooltip(commentIcon);
        }
    }

    private resetBulletin<GElement extends d3.BaseType, Datum, PElement extends d3.BaseType, PDatum>(
        selection: d3.Selection<GElement, Datum, PElement, PDatum>
    ): void {
        // reset the bulletin icon/background
        selection.select('text.bulletin-icon').style('visibility', 'hidden');
        selection.select('rect.bulletin-background').style('visibility', 'hidden');

        // reset any tooltips
        this.resetCanvasTooltip(selection.select<SVGTextElement>('text.bulletin-icon'));
    }

    public canvasTooltip<C, GElement extends d3.BaseType, Datum, PElement extends d3.BaseType, PDatum>(
        type: Type<C>,
        selection: d3.Selection<GElement, Datum, PElement, PDatum>,
        tooltipData: unknown
    ): void {
        let closeTimer = -1;
        let openTimer = -1;
        const overlay = this.overlay;
        let overlayRef: OverlayRef | null = null;
        let positionStrategy: PositionStrategy | null = null;

        const cleanup = () => {
            selection.classed('tooltip-active', false);

            overlayRef?.detach();
            overlayRef?.dispose();
            overlayRef = null;

            positionStrategy?.detach?.();
            positionStrategy?.dispose();
            positionStrategy = null;
        };

        // check to see if there is an active tooltip
        const tooltipActive: boolean = selection.classed('tooltip-active');

        // if there is an existing mouse leave listener, dispatch it so that the tooltip is closed
        if (selection.on('mouseleave')) {
            selection.dispatch('mouseleave');
        }

        selection
            .on('mouseenter', function (this: GElement) {
                if (overlayRef?.hasAttached()) {
                    return;
                }

                if (!overlayRef) {
                    openTimer = window.setTimeout(() => {
                        // mark the tooltip as active so that is can be cleaned up in a subsequent invocation of this method
                        selection.classed('tooltip-active', true);

                        const anchor = d3.select(this).node();
                        if (!(anchor instanceof Element)) {
                            return;
                        }

                        positionStrategy = overlay
                            .position()
                            .flexibleConnectedTo(anchor)
                            .withPositions([
                                {
                                    originX: 'end',
                                    originY: 'bottom',
                                    overlayX: 'start',
                                    overlayY: 'top',
                                    offsetX: 8,
                                    offsetY: 8
                                }
                            ])
                            .withPush(true);

                        overlayRef = overlay.create({ positionStrategy });
                        const tooltipReference = overlayRef.attach(new ComponentPortal(type));
                        tooltipReference.setInput('data', tooltipData);

                        // register mouse events
                        tooltipReference.location.nativeElement.addEventListener('mouseenter', () => {
                            if (closeTimer > 0) {
                                window.clearTimeout(closeTimer);
                                closeTimer = -1;
                            }
                        });
                        tooltipReference.location.nativeElement.addEventListener('mouseleave', () => {
                            cleanup();
                        });
                    }, NiFiCommon.TOOLTIP_DELAY_OPEN_MILLIS);
                }
            })
            .on('mouseleave', function () {
                if (openTimer > 0) {
                    window.clearTimeout(openTimer);
                    openTimer = -1;
                }
                closeTimer = window.setTimeout(() => {
                    cleanup();
                }, NiFiCommon.TOOLTIP_DELAY_OPEN_MILLIS);
            });

        // if the tooltip is already active, dispatch a mouse enter event so that a new tooltip is shown with updated tooltip data
        if (tooltipActive) {
            selection.dispatch('mouseenter');
        }
    }

    public resetCanvasTooltip<GElement extends d3.BaseType, Datum, PElement extends d3.BaseType, PDatum>(
        selection: d3.Selection<GElement, Datum, PElement, PDatum>
    ): void {
        // while tooltips are created dynamically, we need to provide the ability to remove the mouse
        // listener to prevent new tooltips from being created on subsequent mouse enter/leave
        selection.on('mouseenter', null).on('mouseleave', null);
    }

    public formatPredictedDuration(duration: number): string {
        if (duration === 0) {
            return 'now';
        }

        return this.humanizeDuration(duration, {
            round: true
        });
    }
}
