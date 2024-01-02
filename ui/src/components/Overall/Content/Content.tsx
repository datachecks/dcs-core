import * as React from "react";
import { Card, CardContent, CardDescription, CardHeader } from "../../UI/Card";
import styles from "./Content.module.css";

interface IOverallContentProps {
  item: {
    header: string;
    color: string;
    description: string;
    value: number;
    icon: JSX.Element;
  };
}

export const OverallContent: React.FunctionComponent<IOverallContentProps> = ({
  item,
}) => {
  return (
    <Card>
      <CardContent>
        <CardHeader>
          <CardContent>
            <div
              style={{
                backgroundColor: item.color + "80",
              }}
              className={styles.header}
            >
              {item.icon}
              {item.header}
            </div>
          </CardContent>
        </CardHeader>
        <h1 className={styles.score}>{item.value}</h1>
      </CardContent>
      <CardContent>
        <CardDescription>{item.description}</CardDescription>
      </CardContent>
    </Card>
  );
};
