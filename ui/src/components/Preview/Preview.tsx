import React from "react";
import styles from "./Preview.module.css";
import { DashboardMetricOverview } from "../../api/Api";
import Snapshot from "./Snapshot";
import Overview from "./Overview";

interface IPreviewProps {
  dashboard: DashboardMetricOverview;
}

const Preview: React.FC<IPreviewProps> = ({ dashboard }) => {
  // Adding a event listener to the window to get the width of the window, to make the piechart responsive
  let [width, setWidth] = React.useState(window.innerWidth || 1001);
  window.addEventListener("resize", () => setWidth(window.innerWidth));
  // console.log(dashboard);

  return (
    <section className={styles.section}>
      <Overview dashboard={dashboard} width={width} />
      <Snapshot dashboard={dashboard} width={width} />
    </section>
  );
};

export default Preview;
