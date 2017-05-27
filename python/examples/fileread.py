import os
from pyspark import SparkFiles
from pyspark import SparkContext
path = os.path.join("test.txt")
sc = SparkContext()
with open(path, "w") as testFile:
    _ = testFile.write("100")
sc.addFile(path)
def func(iterator):
    with open(SparkFiles.get("test.txt")) as testFile:
        fileVal = int(testFile.readline())
        return [x * fileVal for x in iterator]
print sc.parallelize([1, 2, 3, 4]).mapPartitions(func).collect()
