import React from "react";
import Overview from "../../components/Overview";
import Preview from "../../components/Preview";
import { DashboardMetricOverview } from "../../api/Api";

function Dashboard({
  dashboard,
  setTab,
}: {
  dashboard: DashboardMetricOverview;
  setTab: (value: string) => void;
}) {
  return (
    <>
      <Overview overall={dashboard.overall} setTab={setTab} />
      <Preview dashboard={dashboard} />
    </>
  );
}

export default Dashboard;
