import { TabsOwnProps } from "@mui/material";
import { MetricHealthStatus } from "../api/Api";
import { themeColors } from "../utils/staticData";

export type docRedirect = {
  key: string;
  title: string;
  url: string;
  info: string;
};

export interface ITabsProps {
  value: string;
  setValue: (value: string) => void;
}

export interface MetricHeader {
  [key: string]: {
    header: string;
    filterVariant?: string;
    filterSelectOptions?: string[];
    enableColumnFilter?: boolean;
    enableSorting?: boolean;
  };
}

export const TabsProps: TabsOwnProps = {
  sx: {
    borderColor: "divider",
    "& .MuiTouchRipple-root": {
      color: "rgba(163, 229, 246, 0.376)",
      borderRadius: "10px",
    },
  },
  variant: "scrollable",
  textColor: "inherit",
  TabIndicatorProps: {
    style: {
      background: themeColors.accent + "60",
      borderRadius: "10px",
      height: "100%",
    },
  },
};

export const VerticalTabsProps: TabsOwnProps = {
  sx: {
    borderColor: "divider",
    "& .MuiTouchRipple-root": {
      color: "rgba(163, 229, 246, 0.376)",
      borderRadius: "10px",
    },
  },
  orientation: "vertical",
  variant: "scrollable",
  indicatorColor: "primary",
  textColor: "inherit",
  TabIndicatorProps: {
    style: {
      background: themeColors.accent + "60",
      borderRadius: "10px",
      width: "100%",
    },
  },
};
