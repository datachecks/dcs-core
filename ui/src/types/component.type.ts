import { MetricHealthStatus } from "../api/Api";

export const navURLs = [
  {
    title: "Github",
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
  default = "--primary",
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
  title: string;
  url: string;
}
export const docRedirects: docRedirect[] = [
  {
    title: "RELIABILITY METRICS",
    url: "https://docs.datachecks.io/metrics/reliability/",
  },
  {
    title: "NUMERIC METRICS",
    url: "https://docs.datachecks.io/metrics/numeric_distribution/",
  },
  {
    title: "UNIQUENESS METRICS",
    url: "https://docs.dataquality.ai/docs/metrics/uniqueness-metrics/",
  },
  {
    title: "COMPLETENESS METRICS",
    url: "https://docs.datachecks.io/metrics/completeness/",
  },
  {
    title: "CUSTOM METRICS",
    url: "https://docs.datachecks.io/metrics/combined/",
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
