import { StrictMode } from "react";
import App from "./App";
import { DashboardInfo } from "./api/Api";
import { createRoot } from "react-dom/client";
import "./style/global.css";

export function buildDashboard(data: DashboardInfo, tagId: string) {
  const root = createRoot(document.getElementById(tagId) as HTMLElement);
  root.render(
    <StrictMode>
      <App data={data} />
    </StrictMode>
  );
}

// @ts-ignore
window.buildDashboard = buildDashboard;

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
// serviceWorker.unregister();
