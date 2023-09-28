import React from 'react';
import ReactDOM from 'react-dom';
import App from './App';

import {DashboardInfo} from "./api/Api";


export function buildDashboard(dashboard: DashboardInfo, tagId: string) {
    ReactDOM.render(
        <React.StrictMode>
            <App dashboard={dashboard} />
        </React.StrictMode>,
        document.getElementById(tagId)
    );
}

// @ts-ignore
window.buildDashboard = buildDashboard;

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
// serviceWorker.unregister();
