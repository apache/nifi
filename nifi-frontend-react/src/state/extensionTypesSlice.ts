import { PayloadAction, createSlice } from '@reduxjs/toolkit';

export interface ExtensionTypesState {
  processorTypes?: unknown;
  controllerServiceTypes?: unknown;
  status: 'idle' | 'loading' | 'success' | 'error';
}

const initialState: ExtensionTypesState = {
  status: 'idle'
};

const extensionTypesSlice = createSlice({
  name: 'extensionTypes',
  initialState,
  reducers: {
    setExtensionTypesStatus(state, action: PayloadAction<ExtensionTypesState['status']>) {
      state.status = action.payload;
    },
    setProcessorTypes(state, action: PayloadAction<unknown>) {
      state.processorTypes = action.payload;
    },
    setControllerServiceTypes(state, action: PayloadAction<unknown>) {
      state.controllerServiceTypes = action.payload;
    },
    loadExtensionTypesForCanvas(state) {
      state.status = 'loading';
    }
  }
});

export const {
  setExtensionTypesStatus,
  setProcessorTypes,
  setControllerServiceTypes,
  loadExtensionTypesForCanvas
} =
  extensionTypesSlice.actions;
export default extensionTypesSlice.reducer;
