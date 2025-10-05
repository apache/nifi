
import React from 'react';
import { Provider } from 'react-redux';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import store from './state/store';
import FlowDesigner from './pages/FlowDesigner';

function App() {
  return (
    <Provider store={store}>
      <BrowserRouter>
        <Routes>
          <Route path="/flow-designer/*" element={<FlowDesigner />} />
          {/* Add more routes as needed */}
        </Routes>
      </BrowserRouter>
    </Provider>
  );
}

export default App;
