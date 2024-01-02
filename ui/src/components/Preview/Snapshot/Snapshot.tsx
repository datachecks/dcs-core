import { DashboardMetricOverview } from "../../../api/Api";
import PieChart from "../../Piechart";
import { themeColors } from "../../../utils/staticData";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardSection,
  CardSubtitle,
  CardTitle,
} from "../../Card";

interface ISnapshotProps {
  dashboard: DashboardMetricOverview;
  width: number;
}

export const Snapshot: React.FC<ISnapshotProps> = ({ dashboard, width }) => {
  const data = [
    {
      id: "Unchecked",
      label: "Unchecked Metrics",
      value: dashboard.overall.metric_validation_unchecked,
      color: themeColors.unchecked,
    },
    {
      id: "Success",
      label: "Validation Success",
      value: dashboard.overall.metric_validation_success,
      color: themeColors.success,
    },
    {
      id: "Failed",
      label: "Validation Failure",
      value: dashboard.overall.metric_validation_failed,
      color: themeColors.failed,
    },
  ];

  const health = [
    {
      id: "Healthy",
      label: "Healthy",
      value: dashboard.overall.health_score,
      color: themeColors.success,
    },
    {
      id: "Unhealthy",
      label: "Unhealthy",
      value: 100 - dashboard.overall.health_score,
      color: themeColors.failed,
    },
  ];

  return (
    <Card>
      <CardHeader>
        <CardTitle>Scorecard Snapshot</CardTitle>
        <CardDescription>
          Comprehensive test results and health score overview—your one-stop
          destination for data reliability and performance insights.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <CardSection>
          <PieChart
            data={data}
            metricName="Overall"
            ArcLabel
            key={width}
            small
          />
          <CardSection>
            <CardSubtitle>Test Results</CardSubtitle>
            <CardDescription>
              Quick insights into test outcomes, guiding your next steps for a
              robust and reliable system.
            </CardDescription>
          </CardSection>
        </CardSection>
        <CardSection>
          <PieChart
            data={health}
            metricName="Health"
            percentage
            ArcLabel
            key={width}
            small
          />
          <CardSection>
            <CardSubtitle>Health Score</CardSubtitle>
            <CardDescription>
              Condensed health assessment for instant insights into the
              well-being of your system
            </CardDescription>
          </CardSection>
        </CardSection>
      </CardContent>
    </Card>
  );
};
