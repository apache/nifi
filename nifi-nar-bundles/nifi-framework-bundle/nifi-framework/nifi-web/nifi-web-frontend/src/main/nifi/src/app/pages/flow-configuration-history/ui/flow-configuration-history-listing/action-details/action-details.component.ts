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

import { Component, Inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import {
    ActionEntity,
    ConfigureActionDetails,
    ConnectionActionDetails,
    ExtensionDetails,
    MoveActionDetails,
    PurgeActionDetails,
    RemoteProcessGroupDetails
} from '../../../state/flow-configuration-history-listing';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { PipesModule } from '../../../../../pipes/pipes.module';
import { MatButtonModule } from '@angular/material/button';

@Component({
    selector: 'action-details',
    standalone: true,
    imports: [CommonModule, MatDialogModule, PipesModule, MatButtonModule],
    templateUrl: './action-details.component.html',
    styleUrls: ['./action-details.component.scss']
})
export class ActionDetails {
    constructor(
        @Inject(MAT_DIALOG_DATA) public actionEntity: ActionEntity,
        private nifiCommon: NiFiCommon
    ) {}

    isRemoteProcessGroup(actionEntity: ActionEntity): boolean {
        return actionEntity.action.sourceType === 'RemoteProcessGroup';
    }

    getRemoteProcessGroupDetails(actionEntity: ActionEntity): RemoteProcessGroupDetails | null {
        if (!this.isRemoteProcessGroup(actionEntity)) {
            return null;
        }
        return actionEntity.action.componentDetails as RemoteProcessGroupDetails;
    }

    getExtensionDetails(actionEntity: ActionEntity): ExtensionDetails | null {
        if (this.isRemoteProcessGroup(actionEntity)) {
            return null;
        }
        return actionEntity.action.componentDetails as ExtensionDetails;
    }

    getConfigureActionDetails(actionEntity: ActionEntity): ConfigureActionDetails | null {
        if (actionEntity.action.operation !== 'Configure') {
            return null;
        }
        return actionEntity.action.actionDetails as ConfigureActionDetails;
    }

    getConnectActionDetails(actionEntity: ActionEntity): ConnectionActionDetails | null {
        if (!['Connect', 'Disconnect'].includes(actionEntity.action.operation)) {
            return null;
        }
        return actionEntity.action.actionDetails as ConnectionActionDetails;
    }

    getMoveActionDetails(actionEntity: ActionEntity): MoveActionDetails | null {
        if (actionEntity.action.operation !== 'Move') {
            return null;
        }
        return actionEntity.action.actionDetails as MoveActionDetails;
    }

    getPurgeActionDetails(actionEntity: ActionEntity): PurgeActionDetails | null {
        if (actionEntity.action.operation !== 'Purge') {
            return null;
        }
        return actionEntity.action.actionDetails as PurgeActionDetails;
    }
}
