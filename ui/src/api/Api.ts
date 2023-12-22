export type DashboardInfo = {
  name: string;
  dashboard: DashboardMetricOverview;
  metrics: MetricRow[];
};

export type MetricRow = {
  metric_name: string;
  data_source: string | null;
  metric_type: string;
  is_valid: boolean | null;
  metric_value: string;
  reason: string | null;
};

export type DashboardMetricOverview = {
  overall: MetricHealthStatus;
  reliability: MetricHealthStatus;
  numeric: MetricHealthStatus;
  uniqueness: MetricHealthStatus;
  completeness: MetricHealthStatus;
  custom: MetricHealthStatus;
};

export type MetricHealthStatus = {
  total_metrics: number;
  metric_validation_success: number;
  metric_validation_failed: number;
  metric_validation_unchecked: number;
  health_score: number;
};

export interface Api {
  getDashboard(dashboardId: string): Promise<DashboardInfo>;
}
