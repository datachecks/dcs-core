import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";

import { DashboardMetricOverview } from "../../../../api/Api";
import { VerticalTabsProps } from "../../../../types/component.type";

import styles from "./VerticalTabs.module.css";

interface IMetricVerticalTabsProps {
  data: DashboardMetricOverview;
  value: number;
  handleChange: (event: React.SyntheticEvent, newValue: number) => void;
}

export const MetricVerticalTabs: React.FC<IMetricVerticalTabsProps> = ({
  data,
  value,
  handleChange,
}) => {
  function a11yProps(index: number) {
    return {
      id: `vertical-tab-${index}`,
      "aria-controls": `vertical-tabpanel-${index}`,
    };
  }

  return (
    <div className={styles.verticalTabs}>
      <Tabs {...VerticalTabsProps} value={value} onChange={handleChange}>
        {Object.entries(data)
          .filter(([dataKey]) => dataKey !== "overall")
          .map(([metric_type, metric], index) => (
            <Tab
              label={metric_type}
              aria-label={metric_type}
              key={metric_type}
              sx={{ textTransform: "capitalize" }}
              {...a11yProps(index)}
            />
          ))}
      </Tabs>
    </div>
  );
};
