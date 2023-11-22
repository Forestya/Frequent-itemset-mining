from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from itertools import combinations
from pyspark.sql.window import Window
import sys


def combo(items):
    return [",".join(map(str, sorted(comb))) for comb in combinations(items, 3)]

combinations_udf = udf(combo, ArrayType(StringType()))

def join_items(items):
     return "|".join([item.strip() for item in items.split(",")])

join_items_udf = udf(join_items, StringType())

class Project2:    
    def run(self, inputPath, outputPath, k):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        
        # Load the data from the input path
        data = spark.read.csv(inputPath, header=False)

        # Assign column names
        data = data.withColumnRenamed('_c0', 'InvoiceNo') \
                   .withColumnRenamed('_c1', 'Description') \
                   .withColumnRenamed('_c3', 'Date') \

        # Preprocess the data to get transactions with InvoiceNo, Description, and Month/Year
        transactions = data.select(
            col('InvoiceNo'), 
            col('Description'), 
            split(split(col('Date'), ' ')[0], '/').getItem(1).alias('Month'),
            split(split(col('Date'), ' ')[0], '/').getItem(2).alias('Year')
        ).withColumn("Year", col("Year").cast(IntegerType())) \
         .withColumn("Month", col("Month").cast(IntegerType()))   
        
        # Create item sets by grouping transactions by InvoiceNo and Month/Year, and getting unique items for each group
        item_sets = transactions.groupBy(col('InvoiceNo'), col('Year'), col('Month')) \
                                .agg(collect_list(col('Description')).alias('Items'))

        
        # Generate all 3-combinations of items for each transaction
        item_combo = item_sets.withColumn("Combinations", combinations_udf(col('Items'))) \
                              .select('Year', 'Month', explode(col('Combinations')).alias('Combination'))
        
        
        # Calculate the total number of transactions for each Month/Year
        total_transactions = transactions.groupBy("Year", "Month") \
                                         .agg(countDistinct("InvoiceNo") \
                                         .alias("TotalTransactions"))
        
        # Count the occurrences of each item set
        item_count = item_combo.groupBy('Year', 'Month', 'Combination') \
                               .count()
        
        # Calculate the support for each item set by dividing its count by the total number of transactions for its Month/Year
        support = item_count.join(total_transactions, ['Year', 'Month']) \
                            .withColumn('Support', col('count') / col('TotalTransactions')) \
                            .coalesce(1)
        
        
        # Get the top k item sets with highest support for each Month/Year
        windowSpec = Window.partitionBy(support['Year'], support['Month']).orderBy(support['Support'].desc(), support['Combination'])
        top_k_support = support.withColumn("rank", rank().over(windowSpec)) \
                               .filter(col("rank") <= k) \
                               .withColumn("Output", concat(col('Month'), lit("/"), col('Year'), lit(",("), join_items_udf(col('Combination')), lit("),"), col('Support')))
        
        
        # Save the output to the output path
        top_k_support.select('Output').coalesce(1).write.text(outputPath)

        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3])
