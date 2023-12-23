import React from "react";

import Tabs from "./components/Tabs";
import Navbar from "./components/Navbar";
import Dashboard from "./pages/Dashboard";
import Metrics from "./pages/Metrics/Metrics";

import "./style/global.css";

import { DashboardInfo } from "./api/Api";

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
