"""
DataMind AI — Source Systems Layer

This package simulates the 7 enterprise source systems defined in
arch/03-source-systems.md. Each system has its own Kafka producer,
Avro schemas, and configuration, mirroring a production telecom
environment processing millions of events per day.

Systems:
  crm_system       — Customer Management (customer_topic)
  billing_system   — Mediation & Charging (calls_topic, sms_topic)
  network_system   — Network Monitoring (data_usage_topic, network_metrics_topic)
  payment_gateway  — Payment Processing (payments_topic)
  recharge_platform— Balance Recharge (recharge_topic)
  roaming_system   — Roaming Management (roaming_topic)
  support_system   — Customer Support (tickets_topic)
"""

__version__ = "1.0.0"
