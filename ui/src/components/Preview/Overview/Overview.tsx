import React from "react";
import { DashboardMetricOverview } from "../../../api/Api";
import MetricVerticalTabs from "./MetricVerticalTabs";
import MetricContent from "./MetricContent";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "../../UI/Card";

interface IOverviewProps {
  dashboard: DashboardMetricOverview;
  width: number;
}

export const Overview: React.FC<IOverviewProps> = ({ dashboard, width }) => {
  const [value, setValue] = React.useState(0);

  const handleChange = (event: React.SyntheticEvent, newValue: number) => {
    setValue(newValue);
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Overview</CardTitle>
        <CardDescription>
          Key metrics providing insights into the integrity and uniqueness of
          your data.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <MetricVerticalTabs
          data={dashboard}
          value={value}
          handleChange={handleChange}
        />
        <MetricContent data={dashboard} value={value} width={width} />
      </CardContent>
    </Card>
  );
};
