import com.datastax.spark.connector._
import org.apache.spark.sql.DataFrame

def computeRegularInputs(tx: DataFrame) = {
  tx.withColumn("input", explode(col("inputs")))
    .filter(size(col("input.address")) === 1)
    .select(
      explode(col("input.address")) as "address",
      col("input.value") as "value",
      col("tx_hash"))
    .groupBy("tx_hash", "address")
    .agg(sum("value") as "value")
    .join(
      tx.select(
        col("tx_hash"),
        col("height"),
        col("tx_index"),
        col("timestamp"),
        col("coinjoin")),
      Seq("tx_hash"))
}

def computeRegularOutputs(tx: DataFrame) = {
  tx.select(
    posexplode(col("outputs")) as Seq("n", "output"),
    col("tx_hash"),
    col("height"),
    col("tx_index"),
    col("timestamp"),
    col("coinjoin"))
    .filter(size(col("output.address")) === 1)
    .select(
      col("tx_hash"),
      explode(col("output.address")) as "address",
      col("output.value") as "value",
      col("height"),
      col("tx_index"),
      col("n"),
      col("timestamp"),
      col("coinjoin"))
}

def loadCassandraTable(keyspace: String, table: String) = {
  spark.read.format("org.apache.spark.sql.cassandra")
    .options(Map("keyspace" -> keyspace, "table" -> table))
    .load
}

def storeParquet(df: DataFrame, path: String) = {
  df.write.mode("overwrite")
    .parquet(path)
}

val hdfsPath = "hdfs://spark-master:8020/user/graphsense/disposition-study/"
val keyspace_raw = "btc_raw_prod"
val keyspace = "btc_transformed_20200611"

val tx = loadCassandraTable(keyspace_raw, "transaction")
val addressId = loadCassandraTable(keyspace, "address_by_id_group")
val addressCluster = loadCassandraTable(keyspace, "address_cluster")
val clusterTags = loadCassandraTable(keyspace, "cluster_tags")

val exchangeAddresses = (spark.read.format("csv")
  .option("header", "true")
  .load(f"$hdfsPath/exchange_addresses.csv")
  .join(addressId.select("address", "address_id"), Seq("address"), "left")
  .join(addressCluster.select("address_id", "cluster"), Seq("address_id"), "left")
  .persist())

val exchangeAddressesAgg = (exchangeAddresses
  .groupBy("cluster")
  .agg(concat_ws("|", collect_set("exchange")).as("exchange")))

val exchangeCluster = (clusterTags
  .filter($"category" === "exchange")
  .select("cluster")
  .union(exchangeAddressesAgg.select("cluster"))
  .distinct)

val regularInputs = (computeRegularInputs(tx)
  .join(addressId.select("address", "address_id"), Seq("address"), "left")
  .join(addressCluster.select("address_id", "cluster"), Seq("address_id"), "left")
  .persist())

val regularOutputs = (computeRegularOutputs(tx)
  .join(addressId.select("address", "address_id"), Seq("address"), "left")
  .join(addressCluster.select("address_id", "cluster"), Seq("address_id"), "left")
  .persist())

// txs to exchanges
val regularInputsFiltered = (regularInputs
  .join(exchangeCluster, Seq("cluster"), "left_anti"))
val regularOutputsFiltered = (regularOutputs
  .join(exchangeAddressesAgg.select("cluster", "exchange"), Seq("cluster"), "right"))
val txsToExchanges = (regularOutputsFiltered
  .join(regularInputsFiltered.select("tx_hash").distinct, Seq("tx_hash"), "inner")
  .select("tx_hash", "height", "timestamp", "address", "value", "exchange")
  .distinct
  .withColumn("tx_hash", lower(hex($"tx_hash")))
  .persist())

// txs from exchanges
val regularInputsFiltered = (regularInputs
  .join(exchangeAddressesAgg.select("cluster", "exchange"), Seq("cluster"), "right"))
val regularOutputsFiltered = (regularOutputs
  .join(exchangeCluster, Seq("cluster"), "left_anti"))
val txsFromExchanges = (regularInputsFiltered
  .join(regularOutputsFiltered.select("tx_hash").distinct, Seq("tx_hash"), "inner")
  .select("tx_hash", "height", "timestamp", "address", "value", "exchange")
  .distinct
  .withColumn("tx_hash", lower(hex($"tx_hash")))
  .persist())

exchangeAddresses.count
println("toExchanges")
txsToExchanges.count
println("fromExchanges")
txsFromExchanges.count

storeParquet(exchangeAddresses, f"$hdfsPath/exchange_address_with_cluster.parquet")
storeParquet(txsToExchanges, f"$hdfsPath/txs_to_exchanges.parquet")
storeParquet(txsFromExchanges, f"$hdfsPath/txs_from_exchanges.parquet")
