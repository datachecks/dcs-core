import { ResponsivePie } from "@nivo/pie";

type TPieChartData = {
  id: string;
  label: string;
  value: number;
  color: string;
};

interface IPieChartPreview {
  data: TPieChartData[];
  metricName: string;
  percentage?: boolean;
  ArcLabel?: boolean;
}

const PieChart: React.FC<IPieChartPreview> = ({
  data,
  metricName,
  percentage,
  ArcLabel,
}) => {
  return (
    <ResponsivePie
      key={metricName}
      colors={{
        datum: "data.color",
      }}
      data={data}
      margin={
        !ArcLabel
          ? { top: 0, right: 80, bottom: 0, left: 80 }
          : { top: 50, right: 10, bottom: 10, left: 10 }
      }
      innerRadius={0.5}
      padAngle={0.7}
      cornerRadius={3}
      activeOuterRadiusOffset={8}
      enableArcLinkLabels={!ArcLabel}
      borderWidth={1}
      borderColor={{
        from: "color",
        modifiers: [["darker", 0.2]],
      }}
      arcLabel={(item) => `${item.value} ${percentage ? "%" : ""}`}
      //   arcLabel={(item) => `${item.value}%`}
      arcLinkLabelsSkipAngle={10}
      arcLinkLabelsTextColor="#000"
      arcLinkLabelsThickness={2}
      arcLinkLabelsColor={{ from: "color" }}
      arcLabelsSkipAngle={10}
      arcLabelsTextColor={{
        from: "color",
        modifiers: [["darker", 2]],
      }}
      defs={[
        {
          id: "lines",
          type: "patternLines",
          background: "inherit",
          color: "rgba(255, 255, 255, 0.1)",
          rotation: -45,
          lineWidth: 6,
          spacing: 10,
        },
      ]}
      fill={data.map((item) => ({
        match: {
          id: item.id,
        },
        id: "lines",
      }))}
    />
  );
};

export default PieChart;
