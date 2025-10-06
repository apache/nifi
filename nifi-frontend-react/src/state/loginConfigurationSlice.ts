import { createSlice, PayloadAction } from '@reduxjs/toolkit';

export interface LoginConfiguration {
  loginSupported?: boolean;
  kerberosSupported?: boolean;
  anonymousAuthentication?: boolean;
  [key: string]: unknown;
}

export interface LoginConfigurationState {
  status: 'idle' | 'loading' | 'success' | 'error';
  loginConfiguration?: LoginConfiguration;
  error?: string;
}

const initialState: LoginConfigurationState = {
  status: 'idle'
};

const loginConfigurationSlice = createSlice({
  name: 'loginConfiguration',
  initialState,
  reducers: {
    setLoginConfigurationStatus(state, action: PayloadAction<LoginConfigurationState['status']>) {
      state.status = action.payload;
      if (action.payload !== 'error') {
        state.error = undefined;
      }
    },
    setLoginConfiguration(state, action: PayloadAction<LoginConfiguration | undefined>) {
      state.loginConfiguration = action.payload;
    },
    setLoginConfigurationError(state, action: PayloadAction<string | undefined>) {
      state.error = action.payload ?? 'Unknown error';
      state.status = 'error';
    },
    resetLoginConfiguration() {
      return initialState;
    }
  }
});

export const {
  setLoginConfigurationStatus,
  setLoginConfiguration,
  setLoginConfigurationError,
  resetLoginConfiguration
} = loginConfigurationSlice.actions;

export default loginConfigurationSlice.reducer;
