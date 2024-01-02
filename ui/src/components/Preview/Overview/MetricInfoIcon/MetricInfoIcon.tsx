import IconButton from "@mui/material/IconButton";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import styles from "./MetricInfoIcon.module.css";
import { docRedirects } from "../../../../utils/staticData";
import { BootstrapTooltip } from "../../../Tooltip/BootstrapTooltip";

interface IMetricInfoIconProps {
  metric_type: string;
}

export const MetricInfoIcon: React.FC<IMetricInfoIconProps> = ({
  metric_type,
}) => {
  return (
    <div className={styles.metricInfo}>
      <IconButton
        onClick={() =>
          window.open(
            docRedirects.find((item) => item.key === metric_type)?.url
          )
        }
      >
        <BootstrapTooltip
          title={docRedirects.find((item) => item.key === metric_type)?.info}
        >
          <InfoOutlinedIcon fontSize={"small"} className={styles.infoIcon} />
        </BootstrapTooltip>
      </IconButton>
    </div>
  );
};
