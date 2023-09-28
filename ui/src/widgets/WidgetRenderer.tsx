import React from "react";

import {
    CounterWidgetParams,
    WidgetInfo,
    WidgetSize
} from "../api/Api";
import Widget from "./Widget";
import CounterWidgetContent from "./CounterWidgetContent";
import NotImplementedWidgetContent from "./NotImplementedWidgetContent";

function sizeTransform(size: WidgetSize) : (1 | 3 | 6 | 12) {
    if (size === WidgetSize.Small) {
        return 3;
    } else if (size === WidgetSize.Medium) {
        return 6;
    } else if (size === WidgetSize.Big) {
        return 12;
    }
    return 12;
}

export function WidgetRenderer(key: string, info: WidgetInfo) {
    var content = <NotImplementedWidgetContent />;
    if (info.type === "counter") {
        content = <CounterWidgetContent {...(info.params as CounterWidgetParams)}/>;
    }
    return <Widget key={key} size={sizeTransform(info.size)}>
        {{
            ...info,
            content: content,
        }}
    </Widget>;
}