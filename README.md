# LidarPartitioner
Lidar-Partitioner is a novel tool that enables the partitioning of lidar data by dividing it into several chunks with somewhat the same sizes.


## Lidar algorithms
We applied two algorithms to check the correctness of the partitioning and to measure what is the optimal partition
size of lidar data. These algorithms as follows:

### The Outlier Removal Filter (radius method)
As described in [PDAL: Point cloud Data Abstraction Library](https://pdal.io/PDAL.pdf) page[162], There are two methods to remove the outliers: radius and statistical methods. Here in our experiments, we applied the radius method. This method counts the number of adjacent points <img src="https://render.githubusercontent.com/render/math?math=k_i"> within radius <img src="https://render.githubusercontent.com/render/math?math=r"> for each point <img src="https://render.githubusercontent.com/render/math?math=p_i"> in the input PointView. Then the value of each <img src="https://render.githubusercontent.com/render/math?math=k_i"> and
<img src="https://render.githubusercontent.com/render/math?math=k_{min}"> are compared, so that if <img src="https://render.githubusercontent.com/render/math?math=k_i < k_{min}">, where <img src="https://render.githubusercontent.com/render/math?math=k_{min}"> is the minimum number of neighbors, it is indicated as outlier.



### The Simple Morphological Filter
One of the fundamental problem in terrain classification of LiDAR data is the production of digital elevation models (DEM). The Simple Morphological Filter as explained in [An Improved Simple Morphological Filter for the Terrain Classification of Airborne LIDAR Data](https://www.researchgate.net/publication/258333806_An_Improved_Simple_Morphological_Filter_for_the_Terrain_Classification_of_Airborne_LIDAR_Data) addressed this problem through applying techniques of image processing to the data. This algorithm works as follows: The first step, generation of the minimum surface $ZI_{min}$. The second step, which represents the heart of the algorithm is the tackling of the minimum surface, in which grid cells from the raster are specified as either including objects (OBJ) or bare earth (BE). The third step is the creation of a DEM from the gridded points produced from the previous step. Eventually, based on the produced DEM in the previous step, the original LiDAR points are identified as either BE or  OBJ.


## The resources 

* [PDAL: Point cloud Data Abstraction Library](https://pdal.io/PDAL.pdf)
* [An Improved Simple Morphological Filter for the Terrain Classification of Airborne LIDAR Data](https://www.researchgate.net/publication/258333806_An_Improved_Simple_Morphological_Filter_for_the_Terrain_Classification_of_Airborne_LIDAR_Data)
* [lithops tool](https://github.com/lithops-cloud/lithops)
