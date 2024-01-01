import { ITabsProps, TabsProps } from "../../../types/component.type";
import styles from "./Tabs.module.css";
import { Tab, Tabs as MTabs } from "@mui/material";

export const Tabs = ({ value, setValue }: ITabsProps) => {
  const tabs = ["dashboard", "metrics"];
  return (
    <div className={styles.tabs}>
      <MTabs
        {...TabsProps}
        value={value}
        aria-label="Navigation Tabs"
        onChange={(e: React.SyntheticEvent, v: string) => setValue(v)}
      >
        {tabs.map((item) => (
          <Tab
            value={item}
            label={item}
            key={item}
            sx={{
              padding: "0 2rem",
            }}
          />
        ))}
      </MTabs>
    </div>
  );
};
