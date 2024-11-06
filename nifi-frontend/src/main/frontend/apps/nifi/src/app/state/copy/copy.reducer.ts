/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { CopyState, PasteRequestStrategy } from './index';
import { createReducer, on } from '@ngrx/store';
import { contentPasted, resetCopiedContent, setCopiedContent } from './copy.actions';
import { produce } from 'immer';

export const initialCopyState: CopyState = {
    copiedContent: null
};

export const copyReducer = createReducer(
    initialCopyState,
    on(setCopiedContent, (state, { content }) => ({
        ...state,
        copiedContent: content
    })),
    on(contentPasted, (state, { pasted }) => {
        // update the paste count if it was pasted with an OFFSET strategy to influence positioning of future pastes
        return produce(state, (draftState) => {
            if (
                pasted.strategy === PasteRequestStrategy.OFFSET_FROM_ORIGINAL &&
                draftState.copiedContent &&
                draftState.copiedContent.copyResponse.id === pasted.copyId &&
                draftState.copiedContent.processGroupId === pasted.processGroupId
            ) {
                draftState.copiedContent.pasteCount++;
            }
        });
    }),
    on(resetCopiedContent, () => ({ ...initialCopyState }))
);
