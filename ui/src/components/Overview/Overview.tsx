import React, { useEffect } from "react";
import Score from "./Score";
import { Button } from "@material-ui/core";
import { ArrowForward } from "@material-ui/icons";
import { MetricHealthStatus } from "../../api/Api";
import { overAllScore } from "../../types/component.type";
import styles from "./Overview.module.css";

function Overview({
  overall,
  setTab,
}: {
  overall: MetricHealthStatus;
  setTab: (value: string) => void;
}) {
  return (
    <div className={styles.main}>
      <div className={styles.title}>OVERVIEW</div>
      <div className={styles.overviewWrapper}>
        <div className={styles.content}>
          <div className={styles.description}>
            Datachecks Metric validation is complete! Here's a score of how your
            dataset performed with the validation framework!
          </div>
          <Button
            variant="outlined"
            className={styles.button}
            endIcon={<ArrowForward className={styles.arrow} />}
            onClick={() => setTab("metrics")}
          >
            METRICS
          </Button>
        </div>
        <div className={styles.scoreWrapper}>
          {overAllScore.map((score) => (
            <Score
              {...score}
              score={overall[score.key as keyof MetricHealthStatus]}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

export default Overview;
