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

import { Component, ComponentRef, Input, ViewContainerRef } from '@angular/core';
import { BulletinEntity, ClusterSummary, ControllerStatus } from '../../../state/flow';
import { initialState } from '../../../state/flow/flow.reducer';
import { BulletinsTip } from '../../common/tooltips/bulletins-tip/bulletins-tip.component';

@Component({
    selector: 'flow-status',
    templateUrl: './flow-status.component.html',
    styleUrls: ['./flow-status.component.scss']
})
export class FlowStatus {
    @Input() controllerStatus: ControllerStatus = initialState.flowStatus.controllerStatus;
    @Input() lastRefreshed: string = initialState.flow.processGroupFlow.lastRefreshed;
    @Input() clusterSummary: ClusterSummary = initialState.clusterSummary;
    @Input() bulletins: BulletinEntity[] = initialState.controllerBulletins.bulletins;
    @Input() currentProcessGroupId: string = initialState.id;
    @Input() loadingStatus: boolean = false;

    private closeTimer: number = -1;
    private tooltipRef: ComponentRef<BulletinsTip> | undefined;

    constructor(private viewContainerRef: ViewContainerRef) {}

    hasTerminatedThreads(): boolean {
        return this.controllerStatus.terminatedThreadCount > 0;
    }

    formatClusterMessage(): string {
        if (this.clusterSummary.connectedToCluster && this.clusterSummary.connectedNodes) {
            return this.clusterSummary.connectedNodes;
        } else {
            return 'Disconnected';
        }
    }

    getClusterStyle(): string {
        if (
            !this.clusterSummary.connectedToCluster ||
            this.clusterSummary.connectedNodeCount != this.clusterSummary.totalNodeCount
        ) {
            return 'warning';
        }

        return '';
    }

    formatActiveThreads(): string {
        if (this.hasTerminatedThreads()) {
            return `${this.controllerStatus.activeThreadCount} (${this.controllerStatus.terminatedThreadCount})`;
        } else {
            return `${this.controllerStatus.activeThreadCount}`;
        }
    }

    getActiveThreadsTitle(): string {
        if (this.hasTerminatedThreads()) {
            return 'Active Threads (Terminated)';
        } else {
            return 'Active Threads';
        }
    }

    getActiveThreadsStyle(): string {
        if (this.hasTerminatedThreads()) {
            return 'warning';
        } else if (this.controllerStatus.activeThreadCount === 0) {
            return 'zero';
        }
        return '';
    }

    getQueuedStyle(): string {
        if (this.controllerStatus.queued.indexOf('0 / 0') == 0) {
            return 'zero';
        }
        return '';
    }

    formatValue(value: number | undefined) {
        if (value === undefined) {
            return '-';
        }
        return value;
    }

    getActiveStyle(value: number | undefined, activeStyle: string): string {
        if (value === undefined || value <= 0) {
            return 'zero';
        }
        return activeStyle;
    }

    hasBulletins(): boolean {
        return this.bulletins.length > 0;
    }

    mouseEnter(event: MouseEvent) {
        if (this.hasBulletins()) {
            // @ts-ignore
            const { x, y, width, height } = event.currentTarget.getBoundingClientRect();

            // clear any existing tooltips
            this.viewContainerRef.clear();

            // create and configure the tooltip
            this.tooltipRef = this.viewContainerRef.createComponent(BulletinsTip);
            this.tooltipRef.setInput('top', y + height + 8);
            this.tooltipRef.setInput('left', x + width - 508);
            this.tooltipRef.setInput('data', {
                bulletins: this.bulletins
            });

            // register mouse events
            this.tooltipRef.location.nativeElement.addEventListener('mouseenter', () => {
                if (this.closeTimer > 0) {
                    clearTimeout(this.closeTimer);
                    this.closeTimer = -1;
                }
            });
            this.tooltipRef.location.nativeElement.addEventListener('mouseleave', () => {
                this.tooltipRef?.destroy();
                this.closeTimer = -1;
            });
        }
    }

    mouseLeave(event: MouseEvent) {
        this.closeTimer = setTimeout(() => {
            this.tooltipRef?.destroy();
            this.closeTimer = -1;
        }, 400);
    }
}
