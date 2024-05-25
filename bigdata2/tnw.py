import sys
from operator import add
import re

from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: pyspark <python script> <file>"
        exit(-1)
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    
    # Define a regular expression pattern to match words without numbers
    
    counts = (
        lines.flatMap(lambda x: x.split( ' ')).count()  
    )
     
    f = open("count_all_words.txt", "w")

    f.write("Total words: {}".format(counts))
    f.close()

    sc.stop()