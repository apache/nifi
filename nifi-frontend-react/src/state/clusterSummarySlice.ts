import { PayloadAction, createSlice } from '@reduxjs/toolkit';

export interface ClusterSummaryState {
  disconnectionAcknowledged: boolean;
}

const initialState: ClusterSummaryState = {
  disconnectionAcknowledged: false
};

const clusterSummarySlice = createSlice({
  name: 'clusterSummary',
  initialState,
  reducers: {
    setDisconnectionAcknowledged(state, action: PayloadAction<boolean>) {
      state.disconnectionAcknowledged = action.payload;
    }
  }
});

export const { setDisconnectionAcknowledged } = clusterSummarySlice.actions;
export default clusterSummarySlice.reducer;
