
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

def main():

    print("Script is RUNNING")

    # Initialize spark session
    spark = SparkSession.builder.appName("CustomerOrdersJob").getOrCreate()

    # Set the path
    customers_path = "/opt/spark-apps/input/customers.csv"
    orders_path = "/opt/spark-apps/input/orders.json"
    output_path_csv = "/tmp/orders_enriched_csv"
    output_path_parquet = "/tmp/orders_enriched_parquet"

    # Read the source files
    df_customers = spark.read.option("header", True).csv(customers_path)
    df_orders = spark.read.json(orders_path)

    df_customers.show()
    df_customers.show(4,False)

    df_orders.show(4)
    df_joined = df_orders.join(df_customers,how="inner",on="customer_id")
    df_joined.show()

    # Apply Transformations
    df_enriched = df_joined.withColumn("order_type",
                                    when(df_joined.amount >= 200 ,"High-value")
                                    .when(df_joined.amount >= 100 , "Medimu-value")
                                    .otherwise("Low value")
                                    )
    df_enriched.select("order_id","name","amount","order_type").show()
    df_enriched_op = df_enriched.select("order_id","name","amount","order_type")

    # Write to output
    df_enriched_op.write.mode("overwrite").option("header",True).csv(output_path_csv)
    df_enriched_op.write.mode("overwrite").parquet(output_path_parquet)


    spark.stop()

if __name__ == "__main__":
    main()





