import { MetricHealthStatus } from "../api/Api";
import { themeColors } from "../utils/staticData";

export const navURLs = [
  {
    title: "GitHub",
    logo: "github_logo",
    url: "https://github.com/waterdipai/datachecks",
  },
  {
    title: "Slack",
    logo: "slack_logo",
    url: "https://join.slack.com/t/datachecks/shared_invite/zt-1zqsigy4i-s5aadIh2mjhdpVWU0PstPg",
  },
  {
    title: "Docs",
    logo: "docs_logo",
    url: "https://docs.datachecks.io/",
  },
];

export interface IScoreProps {
  title: string;
  key?: keyof MetricHealthStatus;
  score?: number | string;
  color?: ScoreState | undefined;
  percent?: boolean;
}

export enum ScoreState {
  success = "--success",
  failed = "--failed",
  unchecked = "--unchecked",
}

export const overAllScore: IScoreProps[] = [
  {
    title: "TOTAL METRICS",
    key: "total_metrics",
  },
  {
    title: "PASSED METRICS",
    key: "metric_validation_success",
    color: ScoreState.success,
  },
  {
    title: "UNCHECKED METRICS",
    key: "metric_validation_unchecked",
    color: ScoreState.unchecked,
  },
  {
    title: "FAILED METRICS",
    key: "metric_validation_failed",
    color: ScoreState.failed,
  },
  {
    title: "HEALTH SCORE",
    key: "health_score",
    percent: true,
  },
];
export interface docRedirect {
  key: string;
  title: string;
  url: string;
  info: string;
}
export const docRedirects: docRedirect[] = [
  {
    key: "reliability",
    title: "RELIABILITY METRICS",
    url: "https://docs.datachecks.io/metrics/reliability/",
    info: "Reliability metrics detect whether tables / indices / collections are updating with timely data",
  },
  {
    key: "numeric",
    title: "NUMERIC METRICS",
    url: "https://docs.datachecks.io/metrics/numeric_distribution/",
    info: "Numeric Distribution metrics detect changes in the numeric distributions i.e. of values, variance, skew and more",
  },
  {
    key: "uniqueness",
    title: "UNIQUENESS METRICS",
    url: "https://docs.datachecks.io/metrics/uniqueness/",
    info: "Uniqueness metrics detect when data constraints are breached like duplicates, number of distinct values etc",
  },
  {
    key: "completeness",
    title: "COMPLETENESS METRICS",
    url: "https://docs.datachecks.io/metrics/completeness/",
    info: "Completeness metrics detect when there are missing values in datasets i.e. Null, empty value",
  },
  {
    key: "custom",
    title: "CUSTOM METRICS",
    url: "https://docs.datachecks.io/metrics/combined/",
    info: "Custom metrics detect whether data is formatted correctly and represents a valid value",
  },
];

export enum MetricType {
  reliability = "Reliability",
  numeric = "Numeric",
  uniqueness = "Uniqueness",
  completeness = "Completeness",
  custom = "Custom",
}

export interface ITabsProps {
  value: string;
  setValue: (value: string) => void;
}

export enum PreviewTableHeader {
  metric_type = "Metric Type",
  total_metrics = "Total Metrics",
  metric_validation_success = "Passed Metrics",
  metric_validation_failed = "Failed Metrics",
  metric_validation_unchecked = "Unchecked Metrics",
  health_score = "Health Score",
}

export enum MetricTableHeader {
  metric_name = "Metric Name",
  data_source = "Data Source",
  metric_type = "Metric Type",
  metric_value = "Metric Value",
  is_valid = "Is Valid",
  reason = "Reason",
}

export const TabsProps = {
  sx: {
    borderColor: "divider",
  },
  TabIndicatorProps: {
    style: {
      background: themeColors.success + "60",
      borderRadius: "10px",
      height: "100%",
    },
  },
};

export const VerticalTabsProps = {
  sx: {
    borderColor: "divider",
  },
  TabIndicatorProps: {
    style: {
      background: themeColors.success + "60",
      borderRadius: "10px",
      width: "100%",
    },
  },
};
