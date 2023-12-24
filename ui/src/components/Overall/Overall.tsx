import "../../style/global.css";

import { MetricHealthStatus } from "../../api/Api";
import styles from "./Overall.module.css";

// Icon imports
import SpeedIcon from "@mui/icons-material/Speed";
import HighlightOffIcon from "@mui/icons-material/HighlightOff";
import DesktopAccessDisabledIcon from "@mui/icons-material/DesktopAccessDisabled";
import { themeColors } from "../../utils/staticData";

interface IOverallProps {
  overall: MetricHealthStatus;
}

const Overall: React.FC<IOverallProps> = ({ overall }) => {
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
      value: 12,
      icon: <DesktopAccessDisabledIcon />,
    },
  ];

  return (
    <section className={styles.section}>
      {data.map((item) => (
        <div className={styles.card} key={item.header}>
          <div className={styles.cardSection}>
            <div className={styles.cardInfo}>
              <div className={styles.header}>
                <div
                  style={{
                    backgroundColor: item.color + "80",
                    padding: "0.4rem",
                    borderRadius: "20%",
                  }}
                >
                  {item.icon}
                </div>
                <h1>{item.header}</h1>
              </div>
            </div>
            <h1 className={styles.score}>{item.value}</h1>
          </div>
          <div className={styles.description}>
            <p>{item.description}</p>
          </div>
        </div>
      ))}
    </section>
  );
};

export default Overall;
