#!/bin/bash
# Set up a SINGLE-BROKER Kafka (KRaft) on this EC2 and create the quotes topic. Works on Ubuntu
# (apt) or Amazon Linux (dnf). Advertises the instance PRIVATE IP so Redshift can consume over the
# VPC. Kafka logs -> /data/kafka-logs (short retention; Redshift consumes live). Also creates a
# producer venv with confluent-kafka + pyarrow.
#   sudo bash setup_kafka.sh [port=9092] [topic=quotes] [partitions=6]
set -euo pipefail
KAFKA_VER=3.6.0
PORT="${1:-9092}"; TOPIC="${2:-quotes}"; PARTITIONS="${3:-6}"

TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
PRIV_IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)

# packages (Java + python venv tooling)
if command -v apt-get >/dev/null 2>&1; then
  apt-get update -y >/tmp/kafka-setup.log 2>&1
  apt-get install -y openjdk-17-jre-headless python3-venv python3-pip curl tar gzip >>/tmp/kafka-setup.log 2>&1
  PYBIN=python3
elif command -v dnf >/dev/null 2>&1; then
  dnf install -y java-17-amazon-corretto-headless python3.11 python3.11-pip tar gzip >/tmp/kafka-setup.log 2>&1
  PYBIN=python3.11
else
  echo "no apt/dnf found" >&2; exit 1
fi

mkdir -p /data/kafka-logs
cd /opt
[ -f kafka.tgz ] || curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VER}/kafka_2.13-${KAFKA_VER}.tgz" -o kafka.tgz
tar xzf kafka.tgz && ln -sfn kafka_2.13-${KAFKA_VER} kafka
CFG=/opt/kafka/config/kraft/server.properties
sed -i "s#^advertised.listeners=.*#advertised.listeners=PLAINTEXT://${PRIV_IP}:${PORT}#" "$CFG"
sed -i "s#^listeners=.*#listeners=PLAINTEXT://0.0.0.0:${PORT},CONTROLLER://0.0.0.0:9093#" "$CFG"
sed -i "s#^log.dirs=.*#log.dirs=/data/kafka-logs#" "$CFG"
grep -q '^log.retention.ms=' "$CFG" && sed -i "s#^log.retention.ms=.*#log.retention.ms=1800000#" "$CFG" || echo 'log.retention.ms=1800000' >> "$CFG"
echo 'log.retention.check.interval.ms=60000' >> "$CFG"
export KAFKA_HEAP_OPTS="-Xmx8g -Xms8g"

KID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
/opt/kafka/bin/kafka-storage.sh format -t "$KID" -c "$CFG" --ignore-formatted
nohup /opt/kafka/bin/kafka-server-start.sh "$CFG" > /var/log/kafka.log 2>&1 &
sleep 15
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${PRIV_IP}:${PORT}" \
  --create --topic "${TOPIC}" --partitions "${PARTITIONS}" --replication-factor 1

# producer venv (inherits system pyarrow if present)
$PYBIN -m venv --system-site-packages /opt/producer-venv
/opt/producer-venv/bin/pip install -q --upgrade pip
/opt/producer-venv/bin/pip install -q "confluent-kafka>=2.5.0" "pyarrow>=17.0.0"

echo "================================================================"
echo "broker up:  ${PRIV_IP}:${PORT}   topic=${TOPIC} partitions=${PARTITIONS}"
echo "Redshift KAFKA_BROKERS  ->  ${PRIV_IP}:${PORT}"
echo "run producer with:  /opt/producer-venv/bin/python produce_quotes.py --bootstrap ${PRIV_IP}:${PORT} ..."
echo "================================================================"
