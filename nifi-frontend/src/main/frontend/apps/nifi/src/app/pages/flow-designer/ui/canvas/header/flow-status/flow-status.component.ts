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

import { Component, Input } from '@angular/core';
import { ControllerStatus } from '../../../../state/flow';
import { initialState } from '../../../../state/flow/flow.reducer';
import { BulletinsTip } from '../../../../../../ui/common/tooltips/bulletins-tip/bulletins-tip.component';
import { BulletinsTipInput } from '../../../../../../state/shared';

import { Search } from '../search/search.component';
import { BulletinEntity, NifiTooltipDirective, Storage } from '@nifi/shared';
import { ClusterSummary } from '../../../../../../state/cluster-summary';
import { ConnectedPosition } from '@angular/cdk/overlay';
import { FlowAnalysisState } from '../../../../state/flow-analysis';
import { CommonModule } from '@angular/common';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../../../state';
import { setFlowAnalysisOpen } from '../../../../state/flow/flow.actions';
import { CanvasUtils } from '../../../../service/canvas-utils.service';

@Component({
    selector: 'flow-status',
    templateUrl: './flow-status.component.html',
    imports: [Search, NifiTooltipDirective, CommonModule],
    styleUrls: ['./flow-status.component.scss']
})
export class FlowStatus {
    private static readonly FLOW_ANALYSIS_VISIBILITY_KEY: string = 'flow-analysis-visibility';
    private static readonly FLOW_ANALYSIS_KEY: string = 'flow-analysis';
    public flowAnalysisNotificationClass: string = '';
    @Input() controllerStatus: ControllerStatus = initialState.flowStatus.controllerStatus;
    @Input() lastRefreshed: string = initialState.flow.processGroupFlow.lastRefreshed;
    @Input() clusterSummary: ClusterSummary | null = null;
    @Input() currentProcessGroupId: string = initialState.id;
    @Input() loadingStatus = false;
    @Input() flowAnalysisOpen = initialState.flowAnalysisOpen;
    @Input() set flowAnalysisState(state: FlowAnalysisState) {
        if (!state.ruleViolations.length) {
            this.flowAnalysisNotificationClass = 'primary-color';
        } else {
            const isEnforcedRuleViolated = state.ruleViolations.find((v) => {
                return v.enforcementPolicy === 'ENFORCE';
            });
            isEnforcedRuleViolated
                ? (this.flowAnalysisNotificationClass = 'enforce')
                : (this.flowAnalysisNotificationClass = 'warn');
        }
    }

    @Input() set bulletins(bulletins: BulletinEntity[]) {
        if (bulletins) {
            this.filteredBulletins = bulletins.filter((bulletin) => bulletin.canRead);
        } else {
            this.filteredBulletins = [];
        }
    }

    private filteredBulletins: BulletinEntity[] = initialState.controllerBulletins.bulletins;

    protected readonly BulletinsTip = BulletinsTip;

    constructor(
        private store: Store<NiFiState>,
        private storage: Storage,
        private canvasUtils: CanvasUtils
    ) {
        try {
            const item: { [key: string]: boolean } | null = this.storage.getItem(
                FlowStatus.FLOW_ANALYSIS_VISIBILITY_KEY
            );
            if (item) {
                const flowAnalysisOpen = item[FlowStatus.FLOW_ANALYSIS_KEY] === true;
                this.store.dispatch(setFlowAnalysisOpen({ flowAnalysisOpen }));
            }
        } catch (e) {
            // likely could not parse item... ignoring
        }
    }

    hasTerminatedThreads(): boolean {
        return this.controllerStatus.terminatedThreadCount > 0;
    }

    formatClusterMessage(): string {
        if (this.clusterSummary?.connectedToCluster && this.clusterSummary.connectedNodes) {
            return this.clusterSummary.connectedNodes;
        } else {
            return 'Disconnected';
        }
    }

    getClusterStyle(): string {
        if (
            this.clusterSummary?.connectedToCluster === false ||
            this.clusterSummary?.connectedNodeCount != this.clusterSummary?.totalNodeCount
        ) {
            return 'error-color';
        }

        return 'secondary-color';
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
            return 'zero secondary-color';
        }
        return 'secondary-color';
    }

    getQueuedStyle(): string {
        if (this.controllerStatus.queued.indexOf('0 / 0') == 0) {
            return 'zero secondary-color';
        }
        return 'secondary-color';
    }

    formatValue(value: number | undefined) {
        if (value === undefined) {
            return '-';
        }
        return value;
    }

    getActiveStyle(value: number | undefined, activeStyle: string): string {
        if (value === undefined || value <= 0) {
            return 'zero secondary-color';
        }
        return activeStyle;
    }

    hasBulletins(): boolean {
        return this.filteredBulletins.length > 0;
    }

    getBulletins(): BulletinsTipInput {
        return {
            bulletins: this.filteredBulletins
        };
    }

    getMostSevereBulletinLevel(): string | null {
        // determine the most severe of the bulletins
        const mostSevere = this.canvasUtils.getMostSevereBulletin(this.filteredBulletins);
        return mostSevere ? mostSevere.bulletin.level.toLowerCase() : null;
    }

    getBulletinTooltipPosition(): ConnectedPosition {
        return {
            originX: 'end',
            originY: 'bottom',
            overlayX: 'end',
            overlayY: 'top',
            offsetX: -8,
            offsetY: 8
        };
    }

    toggleFlowAnalysis(): void {
        const flowAnalysisOpen = !this.flowAnalysisOpen;
        this.store.dispatch(setFlowAnalysisOpen({ flowAnalysisOpen }));

        // update the current value in storage
        let item: { [key: string]: boolean } | null = this.storage.getItem(FlowStatus.FLOW_ANALYSIS_VISIBILITY_KEY);
        if (item == null) {
            item = {};
        }

        item[FlowStatus.FLOW_ANALYSIS_KEY] = flowAnalysisOpen;
        this.storage.setItem(FlowStatus.FLOW_ANALYSIS_VISIBILITY_KEY, item);
    }
}
