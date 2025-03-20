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
export class ActionDetails extends CloseOnEscapeDialog {
    constructor(@Inject(MAT_DIALOG_DATA) public actionEntity: ActionEntity) {
        super();
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
        if (!['Connect', 'Disconnect'].includes(action.operation)) {
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
