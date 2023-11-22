from pyspark import SparkContext, SparkConf
import sys
from itertools import combinations
from operator import add
from heapq import nlargest

class Project2:   
        
    def run(self, inputPath, outputPath, k):
        # Set up Spark configuration and context
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)
        
        # Load the data from the input path
        data = sc.textFile(inputPath)

        # Preprocess the data
        transactions = data.map(lambda line: line.split(',')) \
                           .map(lambda fields: (fields[0], fields[1], "/".join(fields[3].split(' ')[0].split('/')[1:3])))  # InvoiceNo, Description, Month/Year
        
        # Create item sets by grouping transactions by InvoiceNo and Month/Year, and getting unique items for each group
        item_sets = transactions.groupBy(lambda x: (x[0], x[2])) \
                                .map(lambda x: (x[0][0], sorted(set([item[1] for item in x[1]])), x[0][1], len(x[1]))) \

        # Generate all 3-combinations of items for each transaction
        item_combo = item_sets.flatMap(lambda x: [(x[2], combo, x[3], x[0]) for combo in combinations(sorted(set(item for item in x[1])), 3)]) \
                              .map(lambda x: ((x[0], x[1]), x[2], x[3]))

        item_set_counts = item_combo.map(lambda x: ((x[0][0], x[0][1]), 1)) \
                                    .reduceByKey(add) \
   
        # Calculate the total number of transactions for each Month/Year
        total_transactions = transactions.map(lambda x: (x[0], x[2])) \
                                         .distinct() \
                                         .map(lambda x: (x[1], 1)) \
                                         .reduceByKey(add)
        
        

        # Calculate the support for each item set by dividing its count by the total number of transactions for its Month/Year
        support = item_set_counts.join(item_combo) \
                                 .map(lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1]))) \
                                 .join(total_transactions) \
                                 .mapValues(lambda x: (x[0], x[0][1] / x[1])) \
                                 .sortBy(lambda x: (int(x[0].split('/')[1]), int(x[0].split('/')[0]), -x[1][1], sorted(x[1][0][0])))
                                

        
        # Format the output
        formatted_output = support.map(lambda x: (",".join((x[0], ('|'.join(x[1][0][0])), str(x[1][1]))))) \
                                  .coalesce(1) \
                                  .distinct()
        
        # Get the top k item sets with highest support for each Month/Year
        top_k_support = formatted_output.groupBy(lambda x: x.split(',')[0]) \
                                        .flatMap(lambda g: nlargest(int(k), g[1], key=lambda x: x[2]))

        
        # Save the output
        top_k_support.coalesce(1) \
                     .saveAsTextFile(outputPath)
        
       
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3])
