#!/bin/bash
set -e

echo "=== Loading Dimensions ==="
spark-submit --master local[*] /home/iceberg/jobs/silver_to_gold/Dims/main.py
echo ""

echo "=== Loading Marts ==="
MARTS=(
    customer_360
    customer_usage_daily
    daily_revenue
    fraud_monitoring
    network_performance
    payment_analytics
    recharge_analytics
    roaming_analytics
    support_analytics
)

for mart in "${MARTS[@]}"; do
    echo "=== $mart ==="
    spark-submit --master local[*] "/home/iceberg/jobs/silver_to_gold/marts/${mart}.py"
    echo ""
done

echo "=== Done ==="

spark-submit --master local[*] "/home/iceberg/jobs/silver_to_gold/marts/customer_360.py