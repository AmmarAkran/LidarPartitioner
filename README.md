# LidarPartitioner
Lidar-Partitioner is a novel tool that enables the partitioning of lidar data by dividing it into several chunks with somewhat the same sizes.


## Lidar algorithms
We applied two algorithms to check the correctness of the partitioning and to measure what is the optimal partition
size of lidar data. These algorithms as follows:

### The Outlier Removal Filter (radius method)
As described in [PDAL: Point cloud Data Abstraction Library](https://pdal.io/PDAL.pdf), There are two method to remove the outliers: radius and statistical methods. Here in our experiments, we applied the radius method 

### The Simple Morphological Filter

## The resources 

* [PDAL: Point cloud Data Abstraction Library](https://pdal.io/PDAL.pdf)
* [An Improved Simple Morphological Filter for the Terrain Classification of Airborne LIDAR Data](https://www.researchgate.net/publication/258333806_An_Improved_Simple_Morphological_Filter_for_the_Terrain_Classification_of_Airborne_LIDAR_Data)
* [lithops tool](https://github.com/lithops-cloud/lithops)
