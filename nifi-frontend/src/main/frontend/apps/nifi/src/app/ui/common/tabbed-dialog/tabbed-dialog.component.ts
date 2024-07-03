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

import { Component, Inject, inject } from '@angular/core';
import { Storage, CloseOnEscapeDialog } from '@nifi/shared';

@Component({
    selector: 'tabbed-dialog',
    standalone: true,
    template: ''
})
export abstract class TabbedDialog extends CloseOnEscapeDialog {
    private storage: Storage = inject(Storage);

    selectedIndex = 0;

    protected constructor(@Inject(String) private dialogId: string) {
        super();

        const previousSelectedIndex = this.storage.getItem<number>(this.dialogId);
        if (previousSelectedIndex != null) {
            this.selectedIndex = previousSelectedIndex;
        }
    }

    tabChanged(selectedTabIndex: number): void {
        this.storage.setItem<number>(this.dialogId, selectedTabIndex);
    }
}
