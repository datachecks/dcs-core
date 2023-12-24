import { docRedirect } from "../types/component.type";

export const themeColors = {
  success: "#72DDF7",
  failed: "#F7AEF8",
  unchecked: "#8093F1",
  error: "#f79898",
};

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
