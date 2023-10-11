import React, { useMemo } from "react";
import { MetricRow } from "../../api/Api";
import { MetricTable } from "../../utils/staticData";
import { MaterialReactTable } from "material-react-table";

function Metrics({ metrics }: { metrics: MetricRow[] }) {
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
}
export default Metrics;
