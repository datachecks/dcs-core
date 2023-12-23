import styles from "../../src/pages/Metrics/Metrics.module.css";
import {
  CancelRounded,
  FilterListRounded,
  HighlightOffOutlined,
  SearchRounded,
} from "@mui/icons-material";
export const MetricTable: {
  Header: MetricHeader;
  Props: any;
} = {
  Header: {
    metric_name: {
      header: "Metric Name",
    },
    data_source: {
      header: "Data Source",
    },
    metric_type: {
      header: "Metric Type",
      filterVariant: "multi-select",
      filterSelectOptions: [
        "avg",
        "min",
        "max",
        "variance",
        "distinct_count",
        "duplicate_count",
        "null_count",
        "null_percentage",
        "empty_string_count",
        "document_count",
        "combined",
      ],
    },
    metric_value: {
      header: "Metric Value",
    },
    is_valid: {
      header: "Valid",
      filterVariant: "select",
      filterSelectOptions: ["True", "False"],
    },
    reason: {
      header: "Reason",
      enableColumnFilter: false,
      enableSorting: false,
    },
  },
  Props: {
    enablePagination: false,
    muiTableProps: {
      className: styles.table,
    },
    muiTableHeadRowProps: {
      className: styles.tableRow,
    },
    muiTableBodyRowProps: {
      className: styles.tableRow,
    },
    enableColumnOrdering: true,
    enableGrouping: true,
    enablePinning: true,
    muiTableHeadCellFilterTextFieldProps: {
      style: {
        textTransform: "capitalize",
      },
      InputLabelProps: {
        style: {
          marginLeft: "1rem",
        },
      },
    },
    icons: {
      SearchIcon: () => <SearchRounded />,
      SearchOffIcon: () => <CancelRounded />,
      FilterListIcon: () => <FilterListRounded />,
      CancelIcon: () => <HighlightOffOutlined fontSize="small" />,
      ClearAllIcon: () => <HighlightOffOutlined fontSize="small" />,
      CloseIcon: () => <HighlightOffOutlined fontSize="small" />,
    },
  },
};

export interface MetricHeader {
  [key: string]: {
    header: string;
    filterVariant?: string;
    filterSelectOptions?: string[];
    enableColumnFilter?: boolean;
    enableSorting?: boolean;
  };
}

export const themeColors = {
  success: "#72DDF7",
  failed: "#F7AEF8",
  unchecked: "#8093F1",
  error: "#f79898",
};
