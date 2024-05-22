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

import { DestroyRef, inject, Injectable } from '@angular/core';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../state';
import { CanvasUtils } from '../canvas-utils.service';
import { PositionBehavior } from '../behavior/position-behavior.service';
import { SelectableBehavior } from '../behavior/selectable-behavior.service';
import { EditableBehavior } from '../behavior/editable-behavior.service';
import * as d3 from 'd3';
import {
    selectFlowLoadingStatus,
    selectRemoteProcessGroups,
    selectAnySelectedComponentIds,
    selectTransitionRequired
} from '../../state/flow/flow.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { QuickSelectBehavior } from '../behavior/quick-select-behavior.service';
import { TextTip } from '../../../../ui/common/tooltips/text-tip/text-tip.component';
import { ValidationErrorsTip } from '../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';
import { Dimension } from '../../state/shared';
import { ComponentType } from '../../../../state/shared';
import { filter, switchMap } from 'rxjs';
import { NiFiCommon } from '../../../../service/nifi-common.service';

@Injectable({
    providedIn: 'root'
})
export class RemoteProcessGroupManager {
    private destroyRef = inject(DestroyRef);

    private dimensions: Dimension = {
        width: 384,
        height: 176
    };

    private static readonly PREVIEW_NAME_LENGTH: number = 30;

    private remoteProcessGroups: [] = [];
    private remoteProcessGroupContainer: any;
    private transitionRequired = false;

    constructor(
        private store: Store<CanvasState>,
        private canvasUtils: CanvasUtils,
        private nifiCommon: NiFiCommon,
        private positionBehavior: PositionBehavior,
        private selectableBehavior: SelectableBehavior,
        private quickSelectBehavior: QuickSelectBehavior,
        private editableBehavior: EditableBehavior
    ) {}

    private select() {
        return this.remoteProcessGroupContainer
            .selectAll('g.remote-process-group')
            .data(this.remoteProcessGroups, function (d: any) {
                return d.id;
            });
    }

    private renderRemoteProcessGroups(entered: any) {
        if (entered.empty()) {
            return entered;
        }

        const remoteProcessGroup = entered
            .append('g')
            .attr('id', function (d: any) {
                return 'id-' + d.id;
            })
            .attr('class', 'remote-process-group component');

        // ----
        // body
        // ----

        // remote process group border
        remoteProcessGroup
            .append('rect')
            .attr('class', 'border')
            .attr('width', function (d: any) {
                return d.dimensions.width;
            })
            .attr('height', function (d: any) {
                return d.dimensions.height;
            })
            .attr('fill', 'transparent')
            .attr('stroke', 'transparent');

        // remote process group body
        remoteProcessGroup
            .append('rect')
            .attr('class', 'body')
            .attr('width', function (d: any) {
                return d.dimensions.width;
            })
            .attr('height', function (d: any) {
                return d.dimensions.height;
            })
            .attr('filter', 'url(#component-drop-shadow)')
            .attr('stroke-width', 0);

        // remote process group name background
        remoteProcessGroup
            .append('rect')
            .attr('class', 'remote-process-group-banner')
            .attr('width', function (d: any) {
                return d.dimensions.width;
            })
            .attr('height', 32);

        // remote process group name
        remoteProcessGroup
            .append('text')
            .attr('x', 30)
            .attr('y', 20)
            .attr('width', 305)
            .attr('height', 16)
            .attr('class', 'remote-process-group-name primary-contrast');

        this.selectableBehavior.activate(remoteProcessGroup);
        this.quickSelectBehavior.activate(remoteProcessGroup);

        return remoteProcessGroup;
    }

    private updateRemoteProcessGroups(updated: any) {
        if (updated.empty()) {
            return;
        }
        const self: RemoteProcessGroupManager = this;

        // remote process group border authorization
        updated.select('rect.border').classed('unauthorized', function (d: any) {
            return d.permissions.canRead === false;
        });

        // remote process group body authorization
        updated.select('rect.body').classed('unauthorized', function (d: any) {
            return d.permissions.canRead === false;
        });

        updated.each(function (this: any, remoteProcessGroupData: any) {
            const remoteProcessGroup: any = d3.select(this);
            let details: any = remoteProcessGroup.select('g.remote-process-group-details');

            // update the component behavior as appropriate
            self.editableBehavior.editable(remoteProcessGroup);

            // if this processor is visible, render everything
            if (remoteProcessGroup.classed('visible')) {
                if (details.empty()) {
                    details = remoteProcessGroup.append('g').attr('class', 'remote-process-group-details');

                    // remote process group transmission status
                    details
                        .append('text')
                        .attr('class', 'remote-process-group-transmission-status')
                        .attr('x', 10)
                        .attr('y', 20);

                    // ------------------
                    // details background
                    // ------------------

                    details
                        .append('rect')
                        .attr('class', 'remote-process-group-details-banner banner')
                        .attr('x', 0)
                        .attr('y', 32)
                        .attr('width', function () {
                            return remoteProcessGroupData.dimensions.width;
                        })
                        .attr('height', 24);

                    // -------
                    // details
                    // -------

                    // remote process group secure transfer
                    details
                        .append('text')
                        .attr('class', 'remote-process-group-transmission-secure')
                        .attr('x', 10)
                        .attr('y', 48);

                    // remote process group uri
                    details
                        .append('text')
                        .attr('x', 30)
                        .attr('y', 48)
                        .attr('width', 305)
                        .attr('height', 12)
                        .attr('class', 'remote-process-group-uri');

                    // ----------------
                    // stats background
                    // ----------------

                    // sent
                    details
                        .append('rect')
                        .attr('class', 'remote-process-group-sent-stats odd')
                        .attr('width', function () {
                            return remoteProcessGroupData.dimensions.width;
                        })
                        .attr('height', 19)
                        .attr('x', 0)
                        .attr('y', 66);

                    // border
                    details
                        .append('rect')
                        .attr('class', 'remote-process-group-stats-border')
                        .attr('width', function () {
                            return remoteProcessGroupData.dimensions.width;
                        })
                        .attr('height', 1)
                        .attr('x', 0)
                        .attr('y', 84);

                    // received
                    details
                        .append('rect')
                        .attr('class', 'remote-process-group-received-stats even')
                        .attr('width', function () {
                            return remoteProcessGroupData.dimensions.width;
                        })
                        .attr('height', 19)
                        .attr('x', 0)
                        .attr('y', 85);

                    // -----
                    // stats
                    // -----

                    // stats label container
                    const remoteProcessGroupStatsLabel = details.append('g').attr('transform', 'translate(6, 75)');

                    // sent label
                    remoteProcessGroupStatsLabel
                        .append('text')
                        .attr('width', 73)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 5)
                        .attr('class', 'stats-label')
                        .text('Sent');

                    // received label
                    remoteProcessGroupStatsLabel
                        .append('text')
                        .attr('width', 73)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 23)
                        .attr('class', 'stats-label')
                        .text('Received');

                    // stats value container
                    const remoteProcessGroupStatsValue = details.append('g').attr('transform', 'translate(95, 75)');

                    // sent value
                    const sentText = remoteProcessGroupStatsValue
                        .append('text')
                        .attr('width', 180)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 5)
                        .attr('class', 'remote-process-group-sent stats-value');

                    // sent count
                    sentText.append('tspan').attr('class', 'count');

                    // sent size
                    sentText.append('tspan').attr('class', 'size');

                    // sent ports
                    sentText.append('tspan').attr('class', 'ports');

                    // received value
                    const receivedText = remoteProcessGroupStatsValue
                        .append('text')
                        .attr('width', 180)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 23)
                        .attr('class', 'remote-process-group-received stats-value');

                    // received ports
                    receivedText.append('tspan').attr('class', 'ports');

                    // received count
                    receivedText.append('tspan').attr('class', 'count');

                    // received size
                    receivedText.append('tspan').attr('class', 'size');

                    // stats value container
                    const processGroupStatsInfo = details.append('g').attr('transform', 'translate(335, 75)');

                    // sent info
                    processGroupStatsInfo
                        .append('text')
                        .attr('width', 25)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 5)
                        .attr('class', 'stats-info')
                        .text('5 min');

                    // received info
                    processGroupStatsInfo
                        .append('text')
                        .attr('width', 25)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 23)
                        .attr('class', 'stats-info')
                        .text('5 min');

                    // -------------------
                    // last refreshed time
                    // -------------------

                    details
                        .append('rect')
                        .attr('class', 'remote-process-group-last-refresh-rect banner')
                        .attr('x', 0)
                        .attr('y', function () {
                            return remoteProcessGroupData.dimensions.height - 24;
                        })
                        .attr('width', function () {
                            return remoteProcessGroupData.dimensions.width;
                        })
                        .attr('height', 24);

                    details
                        .append('text')
                        .attr('x', 10)
                        .attr('y', 168)
                        .attr('class', 'remote-process-group-last-refresh');

                    // --------
                    // comments
                    // --------

                    details
                        .append('text')
                        .attr('class', 'component-comments')
                        .attr(
                            'transform',
                            'translate(' +
                                (remoteProcessGroupData.dimensions.width - 11) +
                                ', ' +
                                (remoteProcessGroupData.dimensions.height - 3) +
                                ')'
                        )
                        .text('\uf075');

                    // -------------------
                    // active thread count
                    // -------------------

                    // active thread count
                    details.append('text').attr('class', 'active-thread-count-icon').attr('y', 20).text('\ue83f');

                    // active thread icon
                    details.append('text').attr('class', 'active-thread-count').attr('y', 20);

                    // ---------
                    // bulletins
                    // ---------

                    // bulletin background
                    details
                        .append('rect')
                        .attr('class', 'bulletin-background')
                        .attr('x', function () {
                            return remoteProcessGroupData.dimensions.width - 24;
                        })
                        .attr('y', 32)
                        .attr('width', 24)
                        .attr('height', 24);

                    // bulletin icon
                    details
                        .append('text')
                        .attr('class', 'bulletin-icon')
                        .attr('x', function () {
                            return remoteProcessGroupData.dimensions.width - 17;
                        })
                        .attr('y', 49)
                        .text('\uf24a');
                }

                if (remoteProcessGroupData.permissions.canRead) {
                    // remote process group uri
                    details
                        .select('text.remote-process-group-uri')
                        .each(function (this: any, d: any) {
                            const remoteProcessGroupUri = d3.select(this);

                            // reset the remote process group name to handle any previous state
                            remoteProcessGroupUri.text(null).selectAll('title').remove();

                            // apply ellipsis to the remote process group name as necessary
                            self.canvasUtils.ellipsis(remoteProcessGroupUri, d.component.targetUris, 'rpg-uri');
                        })
                        .append('title')
                        .text(function (d: any) {
                            return d.component.name;
                        });

                    // update the process groups transmission status
                    details
                        .select('text.remote-process-group-transmission-secure')
                        .text(function (d: any) {
                            let icon: string;
                            if (d.component.targetSecure === true) {
                                icon = '\uf023';
                            } else {
                                icon = '\uf09c';
                            }
                            return icon;
                        })
                        .each(function (this: any, d: any) {
                            self.canvasUtils.canvasTooltip(
                                TextTip,
                                d3.select(this),
                                d.component.targetSecure ? 'Site-to-Site is secure.' : 'Site-to-Site is NOT secure.'
                            );
                        });

                    // ---------------
                    // update comments
                    // ---------------

                    // update the remote process group comments
                    details
                        .select('text.component-comments')
                        .style(
                            'visibility',
                            self.nifiCommon.isBlank(remoteProcessGroupData.component.comments) ? 'hidden' : 'visible'
                        )
                        .each(function (this: any) {
                            if (!self.nifiCommon.isBlank(remoteProcessGroupData.component.comments)) {
                                self.canvasUtils.canvasTooltip(
                                    TextTip,
                                    d3.select(this),
                                    remoteProcessGroupData.component.comments
                                );
                            }
                        });

                    // --------------
                    // last refreshed
                    // --------------

                    details.select('text.remote-process-group-last-refresh').text(function (d: any) {
                        if (d.component.flowRefreshed) {
                            return d.component.flowRefreshed;
                        } else {
                            return 'Remote flow not current';
                        }
                    });

                    // update the process group name
                    remoteProcessGroup
                        .select('text.remote-process-group-name')
                        .each(function (this: any, d: any) {
                            const remoteProcessGroupName = d3.select(this);

                            // reset the remote process group name to handle any previous state
                            remoteProcessGroupName.text(null).selectAll('title').remove();

                            // apply ellipsis to the remote process group name as necessary
                            self.canvasUtils.ellipsis(remoteProcessGroupName, d.component.name, 'rpg-name');
                        })
                        .append('title')
                        .text(function (d: any) {
                            return d.component.name;
                        });
                } else {
                    // clear the target uri
                    details.select('text.remote-process-group-uri').text(null);

                    // clear the transmission secure icon
                    details.select('text.remote-process-group-transmission-secure').text(null);

                    // clear the comments
                    details.select('text.component-comments').style('visibility', 'hidden');

                    // clear the last refresh
                    details.select('text.remote-process-group-last-refresh').text(null);

                    // clear the name
                    remoteProcessGroup.select('text.remote-process-group-name').text(null);
                }

                // populate the stats
                self.updateRemoteProcessGroupStatus(remoteProcessGroup);
            } else {
                if (remoteProcessGroupData.permissions.canRead) {
                    // update the process group name
                    remoteProcessGroup.select('text.remote-process-group-name').text(function (d: any) {
                        const name = d.component.name;
                        if (name.length > RemoteProcessGroupManager.PREVIEW_NAME_LENGTH) {
                            return (
                                name.substring(0, RemoteProcessGroupManager.PREVIEW_NAME_LENGTH) +
                                String.fromCharCode(8230)
                            );
                        } else {
                            return name;
                        }
                    });
                } else {
                    // clear the name
                    remoteProcessGroup.select('text.remote-process-group-name').text(null);
                }

                // remove the details if necessary
                if (!details.empty()) {
                    details.remove();
                }
            }
        });
    }

    private updateRemoteProcessGroupStatus(updated: any) {
        if (updated.empty()) {
            return;
        }
        const self: RemoteProcessGroupManager = this;

        // sent count value
        updated.select('text.remote-process-group-sent tspan.count').text(function (d: any) {
            return self.nifiCommon.substringBeforeFirst(d.status.aggregateSnapshot.sent, ' ');
        });

        // sent size value
        updated.select('text.remote-process-group-sent tspan.size').text(function (d: any) {
            return ' ' + self.nifiCommon.substringAfterFirst(d.status.aggregateSnapshot.sent, ' ');
        });

        // sent ports value
        updated.select('text.remote-process-group-sent tspan.ports').text(function (d: any) {
            return ' ' + String.fromCharCode(8594) + ' ' + d.inputPortCount;
        });

        // received ports value
        updated.select('text.remote-process-group-received tspan.ports').text(function (d: any) {
            return d.outputPortCount + ' ' + String.fromCharCode(8594) + ' ';
        });

        // received count value
        updated.select('text.remote-process-group-received tspan.count').text(function (d: any) {
            return self.nifiCommon.substringBeforeFirst(d.status.aggregateSnapshot.received, ' ');
        });

        // received size value
        updated.select('text.remote-process-group-received tspan.size').text(function (d: any) {
            return ' ' + self.nifiCommon.substringAfterFirst(d.status.aggregateSnapshot.received, ' ');
        });

        // --------------------
        // authorization issues
        // --------------------

        // update the process groups transmission status
        updated
            .select('text.remote-process-group-transmission-status')
            .text(function (d: any) {
                let icon: string;
                if (self.hasIssues(d)) {
                    icon = '\uf071';
                } else if (d.status.transmissionStatus === 'Transmitting') {
                    icon = '\uf140';
                } else {
                    icon = '\ue80a';
                }
                return icon;
            })
            .attr('font-family', function (d: any) {
                let family: string;
                if (self.hasIssues(d) || d.status.transmissionStatus === 'Transmitting') {
                    family = 'FontAwesome';
                } else {
                    family = 'flowfont';
                }
                return family;
            })
            .classed('invalid caution-color', function (d: any) {
                return self.hasIssues(d);
            })
            .classed('transmitting success-color', function (d: any) {
                return !self.hasIssues(d) && d.status.transmissionStatus === 'Transmitting';
            })
            .classed('not-transmitting surface-color', function (d: any) {
                return !self.hasIssues(d) && d.status.transmissionStatus !== 'Transmitting';
            })
            .each(function (this: any, d: any) {
                // if there are validation errors generate a tooltip
                if (d.permissions.canRead && self.hasIssues(d)) {
                    // remote process groups combine validation errors and authorization issues into a single listing
                    self.canvasUtils.canvasTooltip(ValidationErrorsTip, d3.select(this), {
                        isValidating: false,
                        validationErrors: self.getIssues(d)
                    });
                }
            });

        updated.each(function (this: any, d: any) {
            const remoteProcessGroup: any = d3.select(this);

            // -------------------
            // active thread count
            // -------------------

            self.canvasUtils.activeThreadCount(remoteProcessGroup, d);

            // ---------
            // bulletins
            // ---------

            self.canvasUtils.bulletins(remoteProcessGroup, d.bulletins);
        });
    }

    private hasIssues(d: any): boolean {
        return d.status.validationStatus === 'INVALID';
    }

    private getIssues(d: any): any[] {
        let issues: any[] = [];
        if (!this.nifiCommon.isEmpty(d.component.authorizationIssues)) {
            issues = issues.concat(d.component.authorizationIssues);
        }
        if (!this.nifiCommon.isEmpty(d.component.validationErrors)) {
            issues = issues.concat(d.component.validationErrors);
        }
        return issues;
    }

    private removeRemoteProcessGroups(removed: any) {
        removed.remove();
    }

    public init(): void {
        this.remoteProcessGroupContainer = d3
            .select('#canvas')
            .append('g')
            .attr('pointer-events', 'all')
            .attr('class', 'remote-process-groups');

        this.store
            .select(selectRemoteProcessGroups)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((remoteProcessGroups) => {
                this.set(remoteProcessGroups);
            });

        this.store
            .select(selectFlowLoadingStatus)
            .pipe(
                filter((status) => status === 'success'),
                switchMap(() => this.store.select(selectAnySelectedComponentIds)),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe((selected) => {
                this.remoteProcessGroupContainer
                    .selectAll('g.remote-process-group')
                    .classed('selected', function (d: any) {
                        return selected.includes(d.id);
                    });
            });

        this.store
            .select(selectTransitionRequired)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((transitionRequired) => {
                this.transitionRequired = transitionRequired;
            });
    }

    private set(remoteProcessGroups: any): void {
        // update the remote process groups
        this.remoteProcessGroups = remoteProcessGroups.map((remoteProcessGroup: any) => {
            return {
                ...remoteProcessGroup,
                type: ComponentType.RemoteProcessGroup,
                dimensions: this.dimensions
            };
        });

        // select
        const selection = this.select();

        // enter
        const entered = this.renderRemoteProcessGroups(selection.enter());

        // update
        const updated = selection.merge(entered);
        this.updateRemoteProcessGroups(updated);

        // position
        this.positionBehavior.position(updated, this.transitionRequired);

        // exit
        this.removeRemoteProcessGroups(selection.exit());
    }

    public selectAll(): any {
        return this.remoteProcessGroupContainer.selectAll('g.remote-process-group');
    }

    public render(): void {
        this.updateRemoteProcessGroups(this.selectAll());
    }

    public pan(): void {
        this.updateRemoteProcessGroups(
            this.remoteProcessGroupContainer.selectAll(
                'g.remote-process-group.entering, g.remote-process-group.leaving'
            )
        );
    }
}
