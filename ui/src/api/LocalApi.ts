import {Api, DashboardInfo} from "./Api";

export default class LocalApi implements Api {
    private readonly dashboard: DashboardInfo;
    constructor(dashboard: DashboardInfo) {
        this.dashboard = dashboard;
    }

    getDashboard(dashboardId: string): Promise<DashboardInfo> {
        return Promise.resolve(this.dashboard);
    }
}