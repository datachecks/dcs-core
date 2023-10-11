import React from "react";

import Tabs from "./components/Tabs";
import Navbar from "./components/Navbar";
import Dashboard from "./pages/Dashboard";
import Metrics from "./pages/Metrics/Metrics";

import { DashboardInfo } from "./api/Api";

function App(props: { dashboard: DashboardInfo }) {
  const [value, setValue] = React.useState("dashboard");
  return (
    <div>
      <Navbar />
      <Tabs value={value} setValue={setValue} />
      {
        {
          metrics: <Metrics metrics={props.dashboard.metrics} />,
          dashboard: (
            <Dashboard
              setTab={setValue}
              dashboard={props.dashboard.dashboard}
            />
          ),
        }[value]
      }
    </div>
  );
}

export default App;
