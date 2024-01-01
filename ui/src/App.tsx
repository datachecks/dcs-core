import React from "react";

import Dashboard from "./pages/Dashboard";
import Metrics from "./pages/Metrics";

import Navbar from "./components/Navbar";
import Tabs from "./components/UI/Tabs";

import { DashboardInfo } from "./api/Api";

import "./style/global.css";

interface IAppProps {
  data: DashboardInfo;
}

const App: React.FC<IAppProps> = ({ data }) => {
  const [value, setValue] = React.useState("dashboard");

  return (
    <React.Fragment>
      <Navbar />
      <main>
        <Tabs value={value} setValue={setValue} />
        {
          {
            metrics: <Metrics metrics={data.metrics} />,
            dashboard: <Dashboard dashboard={data.dashboard} />,
          }[value]
        }
      </main>
    </React.Fragment>
  );
};

export default App;
