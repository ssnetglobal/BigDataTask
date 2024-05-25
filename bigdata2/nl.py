import sys
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        # print >> sys.stderr, "Usage: pyspark <python script> <file>"
        print("Usage: pyspark <python script> <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    
    #To count the number of lines
    num_lines = lines.count()
    print("Number of lines:", num_lines)
    f = open("total_lines.txt", "w")
    f.write(f"Total Lines : {num_lines}")
    f.close()

    sc.stop()
