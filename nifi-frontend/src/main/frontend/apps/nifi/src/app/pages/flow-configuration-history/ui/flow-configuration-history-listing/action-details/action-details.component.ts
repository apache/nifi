/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, Inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import {
    Action,
    ActionEntity,
    ConfigureActionDetails,
    ConnectionActionDetails,
    ExtensionDetails,
    MoveActionDetails,
    PurgeActionDetails,
    RemoteProcessGroupDetails
} from '../../../state/flow-configuration-history-listing';
import { CloseOnEscapeDialog, CopyDirective } from '@nifi/shared';
import { MatButtonModule } from '@angular/material/button';

@Component({
    selector: 'action-details',
    imports: [CommonModule, MatDialogModule, MatButtonModule, CopyDirective],
    templateUrl: './action-details.component.html',
    styleUrls: ['./action-details.component.scss']
})
export class ActionDetails extends CloseOnEscapeDialog implements OnInit {
    constructor(@Inject(MAT_DIALOG_DATA) public actionEntity: ActionEntity) {
        super();
    }

    ngOnInit(): void {
        // DEBUG: Investigate ActionEntity structure for Process Group information
        console.log('=== INVESTIGATING ACTION ENTITY FOR NIFI-14666 ===');
        console.log('Full actionEntity:', this.actionEntity);
        console.log('ActionEntity keys:', Object.keys(this.actionEntity));

        // Check sourceId (this exists)
        console.log('Source ID:', this.actionEntity.sourceId);

        // Check action object for more details
        if (this.actionEntity.action) {
            console.log('Action object:', this.actionEntity.action);
            console.log('Action keys:', Object.keys(this.actionEntity.action));
            console.log('Action operation:', this.actionEntity.action.operation);

            // Check action properties for source info
            console.log('Action sourceId:', this.actionEntity.action.sourceId);
            console.log('Action sourceName:', this.actionEntity.action.sourceName);
            console.log('Action sourceType:', this.actionEntity.action.sourceType);
        }

        // Look for any process group information
        console.log('Looking for Process Group information...');

        // Check if there's direct process group info in actionEntity
        if ('processGroupId' in this.actionEntity) {
            console.log('Found processGroupId in actionEntity:', (this.actionEntity as any).processGroupId);
        } else {
            console.log('No direct processGroupId in actionEntity');
        }

        // Check if there's process group info in action
        if (this.actionEntity.action && 'processGroupId' in this.actionEntity.action) {
            console.log('Found processGroupId in action:', (this.actionEntity.action as any).processGroupId);
        } else {
            console.log('No processGroupId in action');
        }

        // Check action details for process group info
        if (this.actionEntity.action?.actionDetails) {
            console.log('Action details:', this.actionEntity.action.actionDetails);
            console.log('Action details keys:', Object.keys(this.actionEntity.action.actionDetails));

            if ('processGroupId' in this.actionEntity.action.actionDetails) {
                console.log('Found processGroupId in actionDetails:', (this.actionEntity.action.actionDetails as any).processGroupId);
            }
        }

        // Check component details for process group info
        if (this.actionEntity.action?.componentDetails) {
            console.log('Component details:', this.actionEntity.action.componentDetails);
            console.log('Component details keys:', Object.keys(this.actionEntity.action.componentDetails));

            if ('processGroupId' in this.actionEntity.action.componentDetails) {
                console.log('Found processGroupId in componentDetails:', (this.actionEntity.action.componentDetails as any).processGroupId);
            }
        }

        console.log('=== END INVESTIGATION ===');
    }

    isRemoteProcessGroup(action: Action): boolean {
        return action.sourceType === 'RemoteProcessGroup';
    }

    getRemoteProcessGroupDetails(action: Action): RemoteProcessGroupDetails | null {
        if (!this.isRemoteProcessGroup(action)) {
            return null;
        }
        return action.componentDetails as RemoteProcessGroupDetails;
    }

    getExtensionDetails(action: Action): ExtensionDetails | null {
        if (this.isRemoteProcessGroup(action)) {
            return null;
        }
        return action.componentDetails as ExtensionDetails;
    }

    getConfigureActionDetails(action: Action): ConfigureActionDetails | null {
        if (action.operation !== 'Configure') {
            return null;
        }
        return action.actionDetails as ConfigureActionDetails;
    }

    getConnectActionDetails(action: Action): ConnectionActionDetails | null {
        if (action.operation !== 'Connect' && action.operation !== 'Disconnect') {
            return null;
        }
        return action.actionDetails as ConnectionActionDetails;
    }

    getMoveActionDetails(action: Action): MoveActionDetails | null {
        if (action.operation !== 'Move') {
            return null;
        }
        return action.actionDetails as MoveActionDetails;
    }

    getPurgeActionDetails(action: Action): PurgeActionDetails | null {
        if (action.operation !== 'Purge') {
            return null;
        }
        return action.actionDetails as PurgeActionDetails;
    }
}
