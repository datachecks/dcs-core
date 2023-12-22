import Overall from "../../components/Overall";
import Preview from "../../components/Preview";
import { DashboardMetricOverview } from "../../api/Api";
import React from "react";

interface IDashboardProps {
  dashboard: DashboardMetricOverview;
}

const Dashboard: React.FC<IDashboardProps> = ({ dashboard }) => {
  return (
    <React.Fragment>
      <Preview dashboard={dashboard} />
      <Overall overall={dashboard.overall} />
    </React.Fragment>
  );
};

export default Dashboard;
