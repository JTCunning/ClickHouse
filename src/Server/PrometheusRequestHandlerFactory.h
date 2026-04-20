#pragma once

#include <base/types.h>
#include <memory>


namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

class IServer;
class HTTPRequestHandlerFactory;
using HTTPRequestHandlerFactoryPtr = std::shared_ptr<HTTPRequestHandlerFactory>;
class HTTPRequestHandlerFactoryMain;
class AsynchronousMetrics;

/// Makes a handler factory to handle prometheus protocols.
/// Expects a configuration like this:
///
/// <prometheus>
///     <port>1234</port>
///     <endpoint>/metric</endpoint>
///     <metrics>true</metrics>
///     <asynchronous_metrics>true</asynchronous_metrics>
///     <events>true</events>
///     <errors>true</errors>
/// </prometheus>
///
/// More prometheus protocols can be supported with using a different configuration <prometheus.handlers>
/// (which is similar to the <http_handlers> section):
///
/// <prometheus>
///     <port>1234</port>
///     <handlers>
///         <my_rule1>
///             <url>/metrics</url>
///             <handler>
///                 <type>expose_metrics</type>
///                 <metrics>true</metrics>
///                 <asynchronous_metrics>true</asynchronous_metrics>
///                 <events>true</events>
///                 <errors>true</errors>
///             </handler>
///         </my_rule1>
///    </handlers>
/// </prometheus>
///
/// An alternative port to serve prometheus protocols can be specified in the <protocols> section:
///
/// <protocols>
///     <my_protocol_1>
///         <port>4321</port>
///         <type>prometheus</type>
///     </my_protocol_1>
/// </protocols>
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics,
    const String & name);

/// Makes a HTTP handler factory to handle Prometheus protocol requests for a HTTP rule in the
/// @c http_handlers section. The HTTP-port mount is dynamic-routing-only: the target TimeSeries
/// table is parsed from the URL path segments after the user-configured @c url filter, so
/// @c table and @c database must NOT be specified in the handler config (the factory will reject
/// the rule otherwise). The @c expose_metrics handler type is also rejected here -- it is
/// auto-mounted at @c /metrics by the top-level @c prometheus block. Expects a configuration
/// like this:
///
/// @code{.xml}
/// <http_port>8123</http_port>
/// <http_handlers>
///     <my_rule2>
///         <url>regex:^/foo/[^/]+/[^/]+/write$</url>
///         <methods>POST</methods>
///         <handler>
///             <type>remote_write</type>
///             <http_path_prefix>/foo</http_path_prefix>
///         </handler>
///     </my_rule2>
/// </http_handlers>
/// @endcode
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRule(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix, /// path to "http_handlers.my_handler_1"
    const AsynchronousMetrics & asynchronous_metrics,
    std::unordered_map<String, String> & common_headers);

/// Auto-mounts the three dynamic-routing Prometheus protocol rules (remote_write, remote_read,
/// query_api) on @p factory under the prefix configured by @c prometheus.http_path_prefix
/// (default @c /time-series). No-op when @c prometheus is not configured or the prefix is empty.
/// @c expose_metrics is NOT registered here -- that has its own @c /metrics auto-mount via
/// createPrometheusHandlerFactoryForHTTPRuleDefaults().
void addPrometheusProtocolsToHTTPDefaults(
    HTTPRequestHandlerFactoryMain & factory,
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & async_metrics);

/// Makes a HTTP Handler factory to handle requests for prometheus metrics as a part of the default HTTP rule in the <http_handlers> section.
/// Expects a configuration like this:
///
/// <http_port>8123</http_port>
/// <http_handlers>
///     <defaults/>
/// </http_handlers>
/// <prometheus>
///     <endpoint>/metric</endpoint>
///     <metrics>true</metrics>
///     <asynchronous_metrics>true</asynchronous_metrics>
///     <events>true</events>
///     <errors>true</errors>
/// </prometheus>
///
/// The "defaults" HTTP handler should serve the prometheus exposing metrics protocol on the http port
/// only if it isn't already served on its own port <prometheus.port>,
/// and also if there is no <prometheus.handlers> section in the configuration
/// (because if that section exists then it must be in charge of how prometheus protocols are handled).
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRuleDefaults(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics);

/// Makes a handler factory to handle prometheus protocols.
/// Supports the "expose_metrics" protocol only.
HTTPRequestHandlerFactoryPtr createKeeperPrometheusHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics,
    const String & name);

}
