import React from "react";
import { MetricRow } from "../../api/Api";
import styles from "./Metrics.module.css";
import { MetricTableHeader } from "../../types/component.type";
import { Table, TableCell, TableHead, TableRow } from "@material-ui/core";

function Metrics({ metrics }: { metrics: MetricRow[] }) {
  return (
    <div className={styles.main}>
      <Table className={styles.table}>
        <TableHead>
          <TableRow className={styles.tableRow}>
            {Object.values(MetricTableHeader).map(
              (headerValue: string, index: number) => (
                <TableCell key={index} align="center">
                  {headerValue}
                </TableCell>
              )
            )}
          </TableRow>
        </TableHead>
        {metrics.map((metric, index) => (
          <TableRow className={styles.tableRow}>
            <TableCell key={index} align="center">
              {metric.metric_name}
            </TableCell>
            <TableCell key={index} align="center">
              {metric.data_source ? metric.data_source : "-"}
            </TableCell>
            <TableCell key={index} align="center">
              {metric.metric_type}
            </TableCell>
            <TableCell key={index} align="center">
              {metric.metric_value}
            </TableCell>
            <TableCell key={index} align="center">
              {metric.is_valid !== null
                ? metric.is_valid.toString().toLocaleUpperCase()
                : "-"}
            </TableCell>

            <TableCell key={index} align="center">
              {metric.reason ? metric.reason : "-"}
            </TableCell>
          </TableRow>
        ))}
      </Table>
    </div>
  );
}
export default Metrics;
