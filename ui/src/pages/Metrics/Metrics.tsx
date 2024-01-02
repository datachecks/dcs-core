import React, { useMemo } from "react";
import { MetricRow } from "../../api/Api";
import { MaterialReactTable } from "material-react-table";
import { MetricTable } from "../../utils/metricTable";

interface IMetricsProps {
  metrics: MetricRow[];
}

export const Metrics: React.FC<IMetricsProps> = ({ metrics }) => {
  return (
    <MaterialReactTable
      data={metrics}
      columns={useMemo(
        () =>
          Object.entries(MetricTable.Header).map(([accessor, value]) => ({
            ...value,
            accessorKey: accessor,
          })) as any,
        []
      )}
      {...MetricTable.Props}
    />
  );
};
