import { DashboardMetricOverview } from "../../../../api/Api";
import { themeColors } from "../../../../utils/staticData";

import PieChart from "../../../UI/Piechart";
import MetricInfoIcon from "../MetricInfoIcon";

import styles from "./MetricContent.module.css";

interface IMetricContentProps {
  data: DashboardMetricOverview;
  value: number;
  width: number;
}

export const MetricContent: React.FC<IMetricContentProps> = ({
  data,
  value,
  width,
}) => {
  interface ITabPanelProps {
    children?: React.ReactNode;
    index: number;
    value: number;
  }

  const TabPanel: React.FC<ITabPanelProps> = ({ children, index, value }) => {
    return (
      <div
        role="tabpanel"
        hidden={value !== index}
        id={`vertical-tabpanel-${index}`}
        aria-labelledby={`vertical-tab-${index}`}
      >
        {value === index && (
          <div>
            <p>{children}</p>
          </div>
        )}
      </div>
    );
  };

  return (
    <div className={styles.wrapper}>
      {Object.entries(data)
        .filter(([dataKey]) => dataKey !== "overall")
        .map(([metric_type, metric], index) => {
          const data = [
            {
              id: "Unchecked",
              label: "Unchecked Metrics",
              value: metric.metric_validation_unchecked,
              color: themeColors.unchecked,
            },
            {
              id: "Success",
              label: "Validation Success",
              value: metric.metric_validation_success,
              color: themeColors.success,
            },
            {
              id: "Failed",
              label: "Validation Failure",
              value: metric.metric_validation_failed,
              color: themeColors.failed,
            },
          ];

          return (
            <TabPanel value={value} index={index}>
              <div className={styles.relative}>
                <MetricInfoIcon metric_type={metric_type} />
                <PieChart data={data} metricName={metric_type} key={width} />
              </div>
            </TabPanel>
          );
        })}
    </div>
  );
};
