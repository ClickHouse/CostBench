CREATE TABLE test01 (
    c1 Integer
)
ORDER BY()



INSERT INTO test01 values(1)(2)(3)
SETTINGS
    async_insert=1,
    wait_for_async_insert=0,
    async_insert_deduplicate=0,
    async_insert_busy_timeout_max_ms=10000,
    async_insert_max_data_size=524288000;



curl --user 'ingest:rvxc~0GNrQuNc' \
--data "INSERT INTO test01 SETTINGS async_insert=1, wait_for_async_insert=0, async_insert_deduplicate=0, async_insert_use_adaptive_busy_timeout=0, async_insert_busy_timeout_max_ms=180000, async_insert_max_data_size=524288000 VALUES(1000),(2000),(3000),(4000),(5000),(6000),(7000),(8000),(9000),(90000),(90000),(90000),(90000)" \
https://mc1o88ors2.eu-west-1.aws.clickhouse-staging.com:8443



