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

import { inject, InjectionToken } from '@angular/core';
import { Storage, CloseOnEscapeDialog } from '@nifi/shared';

export const TABBED_DIALOG_ID = new InjectionToken<string>('TABBED_DIALOG_ID');

export abstract class TabbedDialog extends CloseOnEscapeDialog {
    private storage: Storage = inject(Storage);
    private dialogId: string = 'tabbed-dialog-selected-index';

    selectedIndex = 0;

    constructor() {
        super();

        // Inject the dialog ID if provided, otherwise use default
        const injectedDialogId = inject(TABBED_DIALOG_ID, { optional: true });
        if (injectedDialogId !== null && injectedDialogId !== undefined) {
            this.dialogId = injectedDialogId;
        }

        try {
            const previousSelectedIndex = this.storage.getItem<number>(this.dialogId);
            if (previousSelectedIndex != null) {
                this.selectedIndex = previousSelectedIndex;
            }
        } catch (error) {
            // Gracefully handle localStorage errors - use default selectedIndex
        }
    }

    tabChanged(selectedTabIndex: number): void {
        try {
            this.storage.setItem<number>(this.dialogId, selectedTabIndex);
        } catch (error) {
            // Gracefully handle localStorage errors
        }
    }
}
