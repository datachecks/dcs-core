# Telemetry

### What is Telemetry?
Telemetry refers to the collection of usage data. We collect some data to understand how many users we have and how they interact with Datachecks. This helps us improve the tool and prioritize implementing the new features.
Below we describe what is collected, how to opt out and why we'd appreciate if you keep the telemetry on.

### What data is collected?
Datachecks collects anonymous usage data to help our team improve the tool and to apply development efforts to where our users need them most.

We capture one event, when the inspect run is finished. No user data or potentially sensitive information is or ever will be collected. The captured data is limited to:

- Operating System and Python version
- Types of databases used (postgresql, mysql, etc.)
- Number of metrics generated
- Error message, if any, truncated to the first 20 characters.

## How to enable/disable telemetry?

By default, telemetry is enabled.
To disable the data collection you can Set environment variable `DISABLE_DCS_ANONYMOUS_TELEMETRY` to `True`

## Should I opt out?
Being open-source, we have no visibility into the tool usage unless someone actively reaches out to us or opens a GitHub issue.
We’d be grateful if you keep the telemetry on since it helps us answer questions like:

- How many people are actively using the tool?
- Which features are being used most?
- What is the environment you run Datachecks in?

It helps us prioritize the development of new features and make sure we test the performance in the most popular environments.

**We understand that you might still prefer not to share any telemetry data, and we respect this wish. Follow the steps above to disable the data collection.**

