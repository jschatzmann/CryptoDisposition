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
        col("block_id"),
        col("tx_id"),
        col("timestamp"),
        col("coinjoin")),
      Seq("tx_hash"))
}

def computeRegularOutputs(tx: DataFrame) = {
  tx.select(
    posexplode(col("outputs")) as Seq("n", "output"),
    col("tx_hash"),
    col("block_id"),
    col("tx_id"),
    col("timestamp"),
    col("coinjoin"))
    .filter(size(col("output.address")) === 1)
    .select(
      col("tx_hash"),
      explode(col("output.address")) as "address",
      col("output.value") as "value",
      col("block_id"),
      col("tx_id"),
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

val hdfsPath = "HDFS_PATH"
val keyspace_raw = "btc_raw"
val keyspace = "btc_transformed_20211118_dev"

val tx = loadCassandraTable(keyspace_raw, "transaction")
val addressId = loadCassandraTable(keyspace, "address")
val addressCluster = loadCassandraTable(keyspace, "cluster_addresses")
val clusterTags = loadCassandraTable(keyspace, "cluster_tags")

val exchangeAddresses = (spark.read.format("csv")
  .option("header", "true")
  .load(f"$hdfsPath/exchange_addresses.csv")
  .join(addressId.select("address", "address_id"), Seq("address"), "left")
  .join(addressCluster.select("address_id", "cluster_id"), Seq("address_id"), "left")
  .persist())

val exchangeAddressesAgg = (exchangeAddresses
  .groupBy("cluster_id")
  .agg(concat_ws("|", collect_set("exchange")).as("exchange")))

val exchangeCluster = (clusterTags
  .filter($"category" === "exchange")
  .select("cluster_id")
  .union(exchangeAddressesAgg.select("cluster_id"))
  .distinct)

val regularInputs = (computeRegularInputs(tx)
  .join(addressId.select("address", "address_id"), Seq("address"), "left")
  .join(addressCluster.select("address_id", "cluster_id"), Seq("address_id"), "left")
  .persist())

val regularOutputs = (computeRegularOutputs(tx)
  .join(addressId.select("address", "address_id"), Seq("address"), "left")
  .join(addressCluster.select("address_id", "cluster_id"), Seq("address_id"), "left")
  .persist())

// txs to exchanges
val regularInputsFiltered = (regularInputs
  .join(exchangeCluster, Seq("cluster_id"), "left_anti"))
val regularOutputsFiltered = (regularOutputs
  .join(exchangeAddressesAgg.select("cluster_id", "exchange"), Seq("cluster_id"), "right"))
val txsToExchanges = (regularOutputsFiltered
  .join(regularInputsFiltered.select("tx_hash").distinct, Seq("tx_hash"), "inner")
  .select("tx_hash", "block_id", "timestamp", "address", "value", "exchange")
  .distinct
  .withColumn("tx_hash", lower(hex($"tx_hash")))
  .persist())

// txs from exchanges
val regularInputsFiltered = (regularInputs
  .join(exchangeAddressesAgg.select("cluster_id", "exchange"), Seq("cluster_id"), "right"))
val regularOutputsFiltered = (regularOutputs
  .join(exchangeCluster, Seq("cluster_id"), "left_anti"))
val txsFromExchanges = (regularInputsFiltered
  .join(regularOutputsFiltered.select("tx_hash").distinct, Seq("tx_hash"), "inner")
  .select("tx_hash", "block_id", "timestamp", "address", "value", "exchange")
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
