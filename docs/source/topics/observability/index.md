# Observability

Various techniques and tools to monitor and understand the behavior of the driver and the ScyllaDB cluster it interacts with.

> **Note**: In the current release of the driver, only **Logging** is fully implemented and supported. The other features listed below are placeholders for future updates.

* [Metrics](metrics/index.md) - Collecting and exposing metrics about the driver’s performance and behavior.
* [Query Warnings](query-warnings.md)
* [Request Tracker](request-tracker.md)
* [OpenTelemetry](opentelemetry.md)
* [Logging](logging.md)


```{eval-rst}
.. toctree::
   :hidden:
   :glob:

   metrics/index
   query-warnings
   request-tracker
   opentelemetry
   logging
```