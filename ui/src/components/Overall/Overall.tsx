import OverallContent from "./Content";

import SpeedIcon from "@mui/icons-material/Speed";
import HighlightOffIcon from "@mui/icons-material/HighlightOff";
import DesktopAccessDisabledIcon from "@mui/icons-material/DesktopAccessDisabled";

import { themeColors } from "../../utils/staticData";

import { MetricHealthStatus } from "../../api/Api";
import styles from "./Overall.module.css";

interface IOverallProps {
  overall: MetricHealthStatus;
}

export const Overall: React.FC<IOverallProps> = ({ overall }) => {
  const data = [
    {
      header: "Total Metrics",
      color: themeColors.success,
      description:
        "A visual representation of test execution metrics, providing a comprehensive view of your database performance.",
      value: overall.total_metrics,
      icon: <SpeedIcon />,
    },
    {
      header: "Validation Failures",
      color: themeColors.failed,
      description:
        "A breakdown of test failures, empowering you to address issues and enhance database robustness.",
      value: overall.metric_validation_failed,
      icon: <HighlightOffIcon />,
    },
    {
      header: "Unchecked Metrics",
      color: themeColors.unchecked,
      description:
        "Unmonitored tests to ensure comprehensive coverage, all in one glance. Stay ahead of potential blind spots.",
      value: overall.metric_validation_unchecked,
      icon: <DesktopAccessDisabledIcon />,
    },
  ];

  return (
    <section className={styles.section}>
      {data.map((item) => (
        <OverallContent item={item} key={item.header} />
      ))}
    </section>
  );
};
