import React from "react";
import styles from "./Overview.module.css";
import { IScoreProps, ScoreState } from "../../types/component.type";
function Score({
  title,
  score,
  percent,
  color = ScoreState.default,
}: IScoreProps) {
  return (
    <div className={styles.score}>
      <div className={styles.key}>{title}</div>
      <div
        className={styles.value}
        style={{
          color: `var(${color})`,
        }}
      >
        {percent ? `${score}%` : score}
      </div>
    </div>
  );
}

export default Score;
