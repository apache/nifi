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

import { Component, computed, input } from '@angular/core';
import { NgTemplateOutlet } from '@angular/common';
import { ConnectorEntity, ConnectorState } from '../../types';

export type ConnectorStateVariant = 'neutral' | 'critical' | 'caution' | 'success' | 'info';

@Component({
    selector: 'connector-detail-header',
    standalone: true,
    imports: [NgTemplateOutlet],
    templateUrl: './connector-detail-header.component.html',
    styleUrls: ['./connector-detail-header.component.scss']
})
export class ConnectorDetailHeader {
    connector = input.required<ConnectorEntity>();
    condensed = input<boolean>(false);

    simpleType = computed(() => {
        const type = this.connector().component.type || '';
        const parts = type.split('.');
        return parts[parts.length - 1];
    });

    /**
     * Computed signal that checks if the connector has unapplied edits.
     */
    hasUnappliedEdits = computed(() => {
        const action = this.connector().component.availableActions?.find(
            (a) => a.name === 'DISCARD_WORKING_CONFIGURATION'
        );
        return action?.allowed ?? false;
    });

    getStateVariant(state: string): ConnectorStateVariant {
        switch (state) {
            case ConnectorState.RUNNING:
                return 'success';
            case ConnectorState.STOPPED:
            case ConnectorState.DISABLED:
                return 'neutral';
            case ConnectorState.STARTING:
            case ConnectorState.UPDATING:
            case ConnectorState.STOPPING:
            case ConnectorState.DRAINING:
            case ConnectorState.PREPARING_FOR_UPDATE:
                return 'info';
            case ConnectorState.UPDATE_FAILED:
                return 'critical';
            default:
                return 'neutral';
        }
    }

    getStateColorClass(state: string): string {
        switch (this.getStateVariant(state)) {
            case 'success':
                return 'success-color-default';
            case 'critical':
                return 'error-color';
            case 'caution':
                return 'caution-color';
            case 'info':
                return 'primary-color';
            case 'neutral':
            default:
                return 'neutral-color';
        }
    }
}
