import React from "react";

import Navbar from "./components/Navbar";
import Dashboard from "./pages/Dashboard";

import "./style/global.css";

import { DashboardInfo } from "./api/Api";
import Tabs from "./components/UI/Tabs";
import Metrics from "./pages/Metrics";

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
