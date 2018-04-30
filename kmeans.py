import sys
import math
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage: kmeans <fileURL> <k> <e|g>"
        exit(-1)

    distFunc = sys.argv[3]
    if (distFunc != "e" and distFunc != "g"):
        print >> sys.stderr, "distance function could be either e(euclidean) \
                                or g(greate circle)"
        exit(-1)

    sc = SparkContext()

    EARTH_CIRCUMFERENCE = 6378137     # earth circumference in meters

    def greateCircleDistance(p1, p2):
        lat1, lon1 = p1
        lat2, lon2 = p2

        dLat = math.radians(lat2 - lat1)
        dLon = math.radians(lon2 - lon1)
        a = (
            math.sin(dLat / 2) * math.sin(dLat / 2)
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(dLon / 2) * math.sin(dLon / 2))
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        d = EARTH_CIRCUMFERENCE * c

        return d

    def euclideanDistance(p1, p2):
        return (p1[0] - p2[0])**2 + (p1[1] - p2[1])**2

    def addPoints(p1, p2):
        return (p1[0] + p2[0], p1[1] + p2[1])

    def calculateDistance(p1, p2):
        if distFunc == "e":
            return euclideanDistance(p1, p2)
        else:
            return greateCircleDistance(p1, p2)

    def closestPoint(p, points):
        bestIndex = 0
        closestDist = float('inf')
        for i in range(len(points)):
            dist = calculateDistance(p, points[i])
            if dist < closestDist:
                closestDist = dist
                bestIndex = i
        return bestIndex

    def println(x):
        print x

    filename = "file://" + sys.argv[1]
    convergeDist = 0.1
    k = int(sys.argv[2])

    points = sc.textFile(filename)\
        .map(lambda line: line.split(" "))\
        .map(lambda fields: (float(fields[0]), float(fields[1])))\
        .filter(lambda point: not ((point[0] == 0) and (point[1] == 0)))\
        .persist()

    print "number of records: ", points.count()
    kPoints = points.takeSample(False, k, 34)
    print "Starting K points:"
    for p in kPoints:
        print p

    tempDist = float('inf')
    while (tempDist > convergeDist):
        closest = points.map(lambda p: (closestPoint(p, kPoints), (p, 1)))
        pointStats = closest.reduceByKey(
            lambda (point1, n1), (point2, n2): (addPoints(point1, point2),
                                                n1 + n2)
        )
        newKPoints = pointStats.map(
            lambda (i, (point, n)): (i, (point[0] / n, point[1] / n))
        ).collectAsMap()
        tempDist = 0.0
        for i in range(k):
            tempDist += calculateDistance(kPoints[i], newKPoints[i])
        print "Distance between iterations: " + str(tempDist)
        for i in range(k):
            kPoints[i] = newKPoints[i]

    sc.stop()
    print "Final K poitns: "
    for p in kPoints:
        print p

