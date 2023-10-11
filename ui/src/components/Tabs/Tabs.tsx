import React from "react";
import styles from "./Tabs.module.css";
import { Tab, Tabs as MTabs } from "@material-ui/core";
import { ITabsProps } from "../../types/component.type";

function Tabs({ value, setValue }: ITabsProps) {
  return (
    <div className={styles.tabs}>
      <MTabs value={value} onChange={(e, v) => setValue(v)}>
        <Tab value="dashboard" label="DASHBOARD" />
        <Tab value="metrics" label="METRICS" />
      </MTabs>
    </div>
  );
}

export default Tabs;
