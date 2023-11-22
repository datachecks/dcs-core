import React from "react";
import styles from "./Preview.module.css";
import { DashboardMetricOverview } from "../../api/Api";
import { docRedirects, PreviewTableHeader } from "../../types/component.type";
import { Button, Table, TableCell, TableHead, TableRow } from "@mui/material";
import { ArrowForward } from "@mui/icons-material";

function Preview({ dashboard }: { dashboard: DashboardMetricOverview }) {
  const renderTableHeaders = () => (
    <TableHead>
      <TableRow className={styles.tableRow}>
        {Object.values(PreviewTableHeader).map((headerValue: string, index) => (
          <TableCell key={index} align="center">
            {headerValue}
          </TableCell>
        ))}
      </TableRow>
    </TableHead>
  );
  const renderMetricRows = () =>
    Object.entries(dashboard)
      .filter(([dataKey]) => dataKey !== "overall")
      .map(([metric_type, metric], index) => (
        <TableRow key={index} className={styles.tableRow}>
          <TableCell align="center">{metric_type} Metrics</TableCell>
          <TableCell align="center">{metric.total_metrics}</TableCell>
          <TableCell align="center">
            {metric.metric_validation_success +
              metric.metric_validation_failed >
            0
              ? metric.metric_validation_success
              : "-"}
          </TableCell>
          <TableCell align="center">
            {metric.metric_validation_success +
              metric.metric_validation_failed >
            0
              ? metric.metric_validation_failed
              : "-"}
          </TableCell>
          <TableCell align="center">
            {metric.metric_validation_success +
              metric.metric_validation_failed >
            0
              ? `${metric.health_score}%`
              : "-"}
          </TableCell>
        </TableRow>
      ));

  const renderDocRedirects = () =>
    docRedirects.map((doc, index) => {
      return (
        <Button
          endIcon={<ArrowForward />}
          variant="outlined"
          className={styles.button}
          key={index}
          href={doc.url}
          target="_blank"
        >
          {doc.title}
        </Button>
      );
    });

  return (
    <div className={styles.main}>
      <div className={styles.title}>PREVIEW</div>
      <div className={styles.previewWrapper}>
        <Table className={styles.table}>
          {renderTableHeaders()}
          {renderMetricRows()}
        </Table>
        <div className={styles.documentations}>
          Know more about each of the <br /> metric types in Datachecks
          {renderDocRedirects()}
        </div>
      </div>
    </div>
  );
}

export default Preview;
