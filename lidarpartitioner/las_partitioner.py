#import os
from lithops.storage.cloud_proxy import os , cloud_open as open
import time
from math import floor, ceil

import laspy
import numpy as np

#from .write_partitions import write_part


class Partitioner:

    def __init__(self, filename, sufix, partition_type='chips'):

        if filename is None:
            raise Exception("File name is not provided")
        # if not os.path.exists(filename):
        #     raise OSError("No such file or directory: '%s'" % filename)

        #self.filename = os.path.abspath(filename)
        # Absolut path do not work well in cloud storage.
        self.filename = filename

        with open(filename, 'rb') as f:
            self.inFile = laspy.read(f)
        self.data = np.vstack((self.inFile.x, self.inFile.y)).T

        # self.m_xvec = []
        # self.m_yvec = []

        self.m_outViews = []
        self.m_spare = [0] * len(self.inFile)
        print("The points number is %s " % len(self.data))

        self.sufix = sufix
        # load type of partition
        self.partition_type = partition_type

    def view(self):
        return self.inFile

    def emit(self, wide, widemin, widemax):

        n_view = []
        for idx in range(widemin, widemax):
            n_view.append(wide[idx][1])

        return n_view

    def split(self, wide, narrow, spare, pleft, pright, m_partitions):

        left = m_partitions[pleft]
        right = m_partitions[pright]

        if pright - pleft == 1:
            self.m_outViews.append(self.emit(wide, left, right))
        elif pright - pleft == 2:
            centre = m_partitions[pright - 1]
            self.m_outViews.append(self.emit(wide, left, centre))
            self.m_outViews.append(self.emit(wide, centre, right))
        else:
            pcenter = (pleft + pright) // 2
            center = m_partitions[pcenter]

            # We are splitting in the wide direction - split elements in the
            # narrow array by copying them to the spare array in the correct
            # partition.  The spare array then becomes the active narrow array
            # for the [left,right] partition.
            lstart = left
            rstart = center

            for i in range(left, right):
                if narrow[i][2] < center:

                    spare[lstart] = narrow[i]
                    L1 = list(wide[narrow[i][2]])
                    L1[2] = lstart
                    T1 = tuple(L1)
                    wide[narrow[i][2]] = T1

                    lstart += 1
                else:

                    spare[rstart] = narrow[i]
                    L1 = list(wide[narrow[i][2]])
                    L1[2] = rstart
                    T1 = tuple(L1)

                    wide[narrow[i][2]] = T1
                    rstart += 1

            self.decideSplit(wide, spare, narrow, pleft, pcenter, m_partitions)
            self.decideSplit(wide, spare, narrow, pcenter, pright, m_partitions)

        return self.m_outViews

    def decideSplit(self, v1, v2, spare, pleft, pright, m_partitions):
        left = m_partitions[pleft]
        right = m_partitions[pright] - 1

        # Decide the wider direction of the block, and split in that direction
        # to maintain squareness.
        v1range = v1[right][0] - v1[left][0]
        v2range = v2[right][0] - v2[left][0]

        if (v1range > v2range):
            res = self.split(v1, v2, spare, pleft, pright, m_partitions)
        else:
            res = self.split(v2, v1, spare, pleft, pright, m_partitions)

        return res

    @staticmethod
    def partition(size, m_threshold):

        def lround(d):

            if d < 0:
                l = ceil(d - 0.5)
            else:
                l = floor(d + 0.5)
            return l

        num_partitions = size // m_threshold

        if size % m_threshold:
            num_partitions += 1

        total = 0
        partition_size = size / num_partitions
        m_partitions = [0]
        i = 0
        while i < num_partitions:
            total += partition_size
            itotal = lround(total)
            m_partitions.append(itotal)
            i += 1
        return m_partitions

    def load(self):

        xvec = list(zip(self.data[:, 0], range(len(self.data))))
        yvec = list(zip(self.data[:, 1], range(len(self.data))))
        # self.xvec = list(zip(self.data[:, 0], range(len(self.data))))
        # self.yvec = list(zip(self.data[:, 1], range(len(self.data))))

        # Sort xvec and assign other index in yvec to sorted indices in xvec.
        st = time.time()
        xvec.sort()  #
        # print(time.time() - st) #10.140607357025146
        for i, tup in enumerate(xvec):
            idx = tup[1]
            L1 = list(yvec[idx])
            L1.append(i)
            yvec[idx] = tuple(L1)

        # Sort yvec.
        yvec.sort()

        # Iterate through the yvector, setting the xvector appropritary.
        for i, tup in enumerate(yvec):
            idx = tup[2]
            L1 = list(xvec[idx])
            L1.append(i)
            xvec[idx] = tuple(L1)

        return xvec, yvec

    def run(self, capacity):
        if len(self.view()) == 0:
            return
        st = time.time()
        v1, v2 = self.load()
        m_partitions = self.partition(len(self.view()), capacity)
        res = self.decideSplit(v1, v2, self.m_spare, 0, len(m_partitions) - 1, m_partitions)
        print("Preparing all partitions time".format(time.time() - st))
        del v1, v2, m_partitions
        return res

    def make_partition(self, out_dir, capacity=None, buffer=None):
        print('Start partitioning')
        t0 = time.time()
        res = self.run(capacity)
        t1 = time.time()
        print(f'Partitioning time: {t1-t0} s')
        st_up = time.time()
        self.write_part(self.filename, out_dir, res, buffer)
        print("Write part time  {} s".format(time.time() - st_up))
        print('All partitioned files have uploaded')
        return len(self.m_outViews)

    def write_part(self, origfname, out_dir, partitions, buffer):

        # Clean Folder if exists, if not create it
        # TODO line commented to work well with lithops
        # lidar_utils.rem_folder(out_dir)

        from lithops.multiprocessing import Pool
        import re

        import laspy
        import numpy as np
        from lithops.storage.cloud_proxy import cloud_open as open


        def do_partitions(args):
            origfname = args[0]
            fname = args[1]
            out_dir = args[2]
            partition = args[3]
            buffer = args[4]

            with open(origfname, 'rb') as in_file:
                in_las_data = laspy.read(in_file)

            in_data_array = np.vstack((in_las_data.x, in_las_data.y, in_las_data.withheld)).T

            max_values = in_las_data.header.maxs  # [max(shared_array[:,0]), max(shared_array[:,1])]
            min_values = in_las_data.header.mins  # [min(shared_array[:,0]), min(shared_array[:,1])]
            # print('min_values: ', min_values, 'max_values: ', max_values)

            out_las_data = laspy.LasData(in_las_data.header)
            all_indices = np.asarray(range(len(in_las_data.points)))
            keep_points = np.isin(all_indices, partition)
            out_las_data.points = in_las_data.points[keep_points]
            points_inds = np.where(keep_points)
            max_val = [max(out_las_data.x), max(out_las_data.y)]
            min_val = [min(out_las_data.x), min(out_las_data.y)]
            buf_limit = list(zip(min_val, max_val))

            if buffer:

                limit_X_values = buf_limit[0]
                limit_Y_values = buf_limit[1]

                tilX_st = limit_X_values[0]
                tilX_end = limit_X_values[1]

                limitX_bufflow = tilX_st - buffer #if tilX_st > min_values[0] + 0.1 else min_values[0]
                limitX_buffupp = tilX_end + buffer #if tilX_end < max_values[0] - 0.1 else max_values[0]

                tilY_st = limit_Y_values[0]
                tilY_end = limit_Y_values[1]

                limitY_bufflow = tilY_st - buffer #if tilY_st > min_values[1] + 0.1 else min_values[1]
                limitY_buffupp = tilY_end + buffer #if tilY_end < max_values[1] - 0.1 else max_values[1]

                values = np.logical_and(
                    np.logical_and((limitX_bufflow <= in_data_array[:, 0]), (limitX_buffupp >= in_data_array[:, 0])),
                    np.logical_and((limitY_bufflow <= in_data_array[:, 1]), (limitY_buffupp >= in_data_array[:, 1])))
                values_ind = np.where(values)
                keep_buffer_indices = np.setdiff1d(values_ind, partition)
                # print(len(keep_buffer_indices))

                all_values = np.union1d(keep_buffer_indices, points_inds)
                withh = np.isin(all_values, keep_buffer_indices).astype(int)

                try:
                    out_las_data.points = in_las_data.points[all_values]
                    out_las_data.withheld = withh

                    with open(out_dir + '/' + fname, 'wb') as f:
                        out_las_data.write(f)

                except:
                    print("Something wrong")
                    # os.remove(out_dir + '/' + fname)

            else:
                out_las_data.points = in_las_data.points[keep_points]
                with open(out_dir + '/' + fname, 'wb') as f:
                    out_las_data.write(f)

            return True

        args = []
        for i, part in enumerate(partitions):
            fname = re.sub(r'\\', '/', origfname).split('/')[-1].split('.')[0] + \
                    '_' + str(self.sufix).zfill(3) + \
                    '-' + str(i).zfill(4) + \
                    '.las' if '\\' in origfname else origfname.split('/')[-1].split('.')[0] + \
                    '_' + str(self.sufix).zfill(3) + \
                    '-' + str(i).zfill(4) + \
                    '.las'
            args.append((origfname, fname, out_dir, part, buffer))

        with Pool() as pool:
            pool.map(do_partitions, args)
