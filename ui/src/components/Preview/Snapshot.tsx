import styles from "./Preview.module.css";
import { DashboardMetricOverview } from "../../api/Api";
import PieChart from "../Piechart";

interface ISnapshotProps {
  dashboard: DashboardMetricOverview;
  width: number;
}

const Snapshot: React.FC<ISnapshotProps> = ({ dashboard, width }) => {
  const data = [
    {
      id: "Unchecked",
      label: "Unchecked Metrics",
      value: dashboard.overall.metric_validation_unchecked,
      color: "#8093F1",
    },
    {
      id: "Success",
      label: "Validation Success",
      value: dashboard.overall.metric_validation_success,
      color: "#72DDF7",
    },
    {
      id: "Failed",
      label: "Validation Failure",
      value: dashboard.overall.metric_validation_failed,
      color: "#F7AEF8",
    },
  ];

  const health = [
    {
      id: "Healthy",
      label: "Healthy",
      value: dashboard.overall.health_score,
      color: "#72DDF7",
    },
    {
      id: "Unhealthy",
      label: "Unhealthy",
      value: 100 - dashboard.overall.health_score,
      color: "#F7AEF8",
    },
  ];

  return (
    <div className={styles.card}>
      <div className={styles.cardInfo}>
        <h1 className={styles.header}>Scorecard Snapshot</h1>
        <p className={styles.description}>
          Comprehensive test results and health score overview—your one-stop
          destination for data reliability and performance insights.
        </p>
      </div>
      <div className={styles.snapscoreMain}>
        <div className={styles.cardInfo}>
          <div className={styles.snapscoreGraph}>
            {/* setting key to width, to trigger re-render of piechart */}
            <PieChart data={data} metricName="Overall" ArcLabel key={width} />
          </div>
          <div className={styles.cardInfo}>
            <h1 className={styles.subHeader}>Test Results</h1>
            <p className={styles.description}>
              Quick insights into test outcomes, guiding your next steps for a
              robust and reliable system.
            </p>
          </div>
        </div>
        <div className={styles.cardInfo}>
          <div className={styles.snapscoreGraph}>
            {/* setting key to width, to trigger re-render of piechart */}
            <PieChart
              data={health}
              metricName="Health"
              percentage
              ArcLabel
              key={width}
            />
          </div>
          <div className={styles.cardInfo}>
            <h1 className={styles.subHeader}>Health Score</h1>
            <p className={styles.description}>
              Condensed health assessment for instant insights into the
              well-being of your system
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Snapshot;
