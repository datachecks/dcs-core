import "../../style/global.css";

import { MetricHealthStatus } from "../../api/Api";
import styles from "./Overall.module.css";

// Icon imports
import SpeedIcon from "@mui/icons-material/Speed";
import HighlightOffIcon from "@mui/icons-material/HighlightOff";
import DesktopAccessDisabledIcon from "@mui/icons-material/DesktopAccessDisabled";
import { themeColors } from "../../utils/staticData";
import { Card, CardContent, CardDescription, CardHeader } from "../UI/Card";

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
      value: overall.metric_validation_unchecked,
      icon: <DesktopAccessDisabledIcon />,
    },
  ];

  return (
    <section className={styles.section}>
      {data.map((item) => (
        <Card key={item.header}>
          <CardContent>
            <CardHeader>
              <CardContent>
                <div
                  style={{
                    backgroundColor: item.color + "80",
                  }}
                  className={styles.header}
                >
                  {item.icon}
                  {item.header}
                </div>
              </CardContent>
            </CardHeader>
            <h1 className={styles.score}>{item.value}</h1>
          </CardContent>
          <CardContent>
            <CardDescription>{item.description}</CardDescription>
          </CardContent>
        </Card>
      ))}
    </section>
  );
};

export default Overall;
