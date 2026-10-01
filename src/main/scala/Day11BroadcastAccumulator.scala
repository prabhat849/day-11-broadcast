import org.apache.spark.sql.SparkSession

object Day11BroadcastAccumulator {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day11-Broadcast-Accumulator")
      .master("local[4]")
      .getOrCreate()

    // Hide Spark INFO logs
    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext

    println()
    println("======================================================")
    println("       DAY 11 - BROADCAST & ACCUMULATORS")
    println("======================================================")

    // --------------------------------------------------
    // 1. Product Reference Data
    // --------------------------------------------------

    val productReference = Map(
      "P001" -> "Laptop",
      "P002" -> "Mobile",
      "P003" -> "Keyboard",
      "P004" -> "Mouse",
      "P005" -> "Monitor",
      "P006" -> "Printer"
    )

    println()
    println("Product Reference Table:")
    println("------------------------")

    productReference.foreach {
      case (id, name) =>
        println(s"$id -> $name")
    }

    // --------------------------------------------------
    // 2. Broadcast Variable
    // --------------------------------------------------

    val broadcastProducts = sc.broadcast(productReference)

    println()
    println("Broadcast Variable:")
    println("-------------------")
    println("Product reference data broadcast to executors.")

    // --------------------------------------------------
    // 3. Transaction Data - 12 Records
    // --------------------------------------------------

    val transactions = Seq(
      ("T001", "P001", 2, 50000),
      ("T002", "P002", 3, 20000),
      ("T003", "P003", 5, 1500),
      ("T004", "P999", 1, 1000),
      ("T005", "P004", 4, 800),
      ("T006", "P888", 2, 500),
      ("T007", "P005", 2, 15000),
      ("T008", "P002", 1, 20000),
      ("T009", "P777", 3, 1200),
      ("T010", "P006", 2, 12000),
      ("T011", "P001", 1, 50000),
      ("T012", "P666", 4, 700)
    )

    val transactionRDD = sc.parallelize(transactions, 4)

    println()
    println("Total Transactions: " + transactions.size)
    println("RDD Partitions: " + transactionRDD.getNumPartitions)

    // --------------------------------------------------
    // 4. Accumulator
    // --------------------------------------------------

    val badRecords = sc.longAccumulator("Bad Records")

    // --------------------------------------------------
    // 5. Validate Transactions
    // --------------------------------------------------

    val validatedTransactions = transactionRDD.map {

      case (transactionId, productId, quantity, price) =>

        if (broadcastProducts.value.contains(productId)) {

          val productName =
            broadcastProducts.value(productId)

          (transactionId, productId, productName, quantity, price, "VALID")

        } else {

          badRecords.add(1)

          (transactionId, productId, "UNKNOWN", quantity, price, "INVALID")
        }
    }

    // --------------------------------------------------
    // 6. Display Results
    // --------------------------------------------------

    println()
    println("======================================================")
    println("              VALIDATION RESULTS")
    println("======================================================")

    println(
      f"${"ID"}%-6s ${"Product"}%-8s ${"Name"}%-12s ${"Qty"}%-5s ${"Price"}%-10s ${"Status"}%-8s"
    )

    println("-" * 60)

    validatedTransactions.collect().foreach {

      case (id, productId, name, quantity, price, status) =>

        println(
          f"$id%-6s $productId%-8s $name%-12s $quantity%-5d $price%-10d $status%-8s"
        )
    }

    // --------------------------------------------------
    // 7. Accumulator Result
    // --------------------------------------------------

    val totalRecords = transactions.size
    val invalidRecords = badRecords.value
    val validRecords = totalRecords - invalidRecords

    println()
    println("======================================================")
    println("              ACCUMULATOR SUMMARY")
    println("======================================================")

    println(s"Total Records   : $totalRecords")
    println(s"Valid Records   : $validRecords")
    println(s"Bad Records     : $invalidRecords")

    println()
    println("Invalid Product IDs:")
    println("--------------------")

    validatedTransactions.collect()
      .filter(_._6 == "INVALID")
      .foreach {
        case (id, productId, _, _, _, _) =>
          println(s"$id -> $productId")
      }

    println()
    println("======================================================")
    println("Day 11 Completed Successfully")
    println("======================================================")

    spark.stop()
  }
}
