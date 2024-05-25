import sys
import re
from operator import add

from pyspark import SparkContext

def preprocess(line):
    # Remove special characters and numbers, convert to lowercase
    line = re.sub(r'[^A-Za-z\s]', '', line).lower()
    return line

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: pyspark <python script> <file>"
        # print("Usage: pyspark <python script> <file>", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext(appName="PythonWordCount")
    
    lines = sc.textFile(sys.argv[1], 1)
    words = lines.flatMap(lambda x: preprocess(x).split())
    counts = words.map(lambda x: (x, 1)).reduceByKey(add)
    output = counts.sortBy(lambda x: x[0]).collect()
    
    with open("wc_no_space.txt", "w") as f:
        for (word, count) in output:
            f.write("%s: %i\n" % (word, count))
    
    sc.stop()
