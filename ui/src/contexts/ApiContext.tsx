import React from "react";

import {Api, DashboardInfo, WidgetInfo} from "../api/Api";

interface ApiContextState {
    Api: Api;
}

class NotImplementedApi implements Api {

    getDashboard(dashboardId: string): Promise<DashboardInfo> {
        return Promise.reject("not implemented");
    }

}

const ApiContext = React.createContext<ApiContextState>({Api: new NotImplementedApi()});

export default ApiContext;