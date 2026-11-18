SELECT * FROM prometheusQueryRange('ts', '{__name__=~"metric_.*",namespace="ns3"}', 1794988800, 1794999600, 30)
