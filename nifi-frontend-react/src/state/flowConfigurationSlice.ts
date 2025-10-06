import { createSlice, PayloadAction } from '@reduxjs/toolkit';

export interface FlowConfiguration {
  supportsManagedAuthorizer?: boolean;
  supportsConfigurableAuthorizer?: boolean;
  supportsManagedTenant?: boolean;
  supportsConfigurableUsersAndGroups?: boolean;
  supportsConfigurablePolicies?: boolean;
  contentViewerUrl?: string;
  provenanceEnabled?: boolean;
  [key: string]: unknown;
}

export interface FlowConfigurationState {
  status: 'idle' | 'loading' | 'success' | 'error';
  flowConfiguration?: FlowConfiguration;
  error?: string;
}

const initialState: FlowConfigurationState = {
  status: 'idle'
};

const flowConfigurationSlice = createSlice({
  name: 'flowConfiguration',
  initialState,
  reducers: {
    setFlowConfigurationStatus(state, action: PayloadAction<FlowConfigurationState['status']>) {
      state.status = action.payload;
      if (action.payload !== 'error') {
        state.error = undefined;
      }
    },
    setFlowConfiguration(state, action: PayloadAction<FlowConfiguration | undefined>) {
      state.flowConfiguration = action.payload;
    },
    setFlowConfigurationError(state, action: PayloadAction<string | undefined>) {
      state.error = action.payload ?? 'Unknown error';
      state.status = 'error';
    },
    resetFlowConfiguration() {
      return initialState;
    }
  }
});

export const {
  setFlowConfigurationStatus,
  setFlowConfiguration,
  setFlowConfigurationError,
  resetFlowConfiguration
} = flowConfigurationSlice.actions;

export default flowConfigurationSlice.reducer;
