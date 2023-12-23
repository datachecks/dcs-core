import styles from "./Tabs.module.css";
import { Tab, Tabs as MTabs } from "@mui/material";
import { ITabsProps, TabsProps } from "../../types/component.type";

function Tabs({ value, setValue }: ITabsProps) {
  return (
    <div className={styles.tabs}>
      <MTabs
        {...TabsProps}
        value={value}
        variant="scrollable"
        aria-label="Vertical tabs example"
        textColor="inherit"
        onChange={(e: any, v: any) => setValue(v)}
      >
        <Tab
          value="dashboard"
          label="DASHBOARD"
          sx={{
            padding: "0 2rem",
          }}
        />
        <Tab
          value="metrics"
          label="METRICS"
          sx={{
            padding: "0 2rem",
          }}
        />
      </MTabs>
    </div>
  );
}

export default Tabs;
