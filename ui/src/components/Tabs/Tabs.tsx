import React from "react";
import styles from "./Tabs.module.css";
import { Tab, Tabs as MTabs } from "@mui/material";
import { ITabsProps, TabsProps } from "../../types/component.type";

function Tabs({ value, setValue }: ITabsProps) {
  return (
    <div className={styles.tabs}>
      <MTabs
        {...TabsProps}
        value={value}
        onChange={(e: any, v: any) => setValue(v)}
      >
        <Tab value="dashboard" label="DASHBOARD" />
        <Tab value="metrics" label="METRICS" />
      </MTabs>
    </div>
  );
}

export default Tabs;
