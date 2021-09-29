from logging import exception
import shlex
import shutil
import subprocess
import time, os

import laspy
import matplotlib.pyplot as plt
import numpy as np
from lithops import Storage
from lithops.storage.cloud_proxy import cloud_open as open_cl
# from lithops.storage.cloud_proxy import os
from lithops.storage.cloud_proxy import CloudStorage, CloudFileProxy

storage = Storage()

_storage = CloudStorage(Storage)
os_cl = CloudFileProxy(_storage)

def clean_folder(folder_name):

    # print(os_cl.path.exists(folder_name))
    if os_cl.path.exists(folder_name):
        for filename in os_cl.listdir(folder_name):
            file_path = folder_name + '/' + filename
            # print(file_path)
            try:
                if os_cl.path.isfile(file_path):
                    os_cl.remove(file_path)
                elif os_cl.path.isdir(file_path):
                    for filename in os_cl.listdir(file_path):
                        os_cl.remove(file_path + '/' + filename)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))
    else:
        print(f"{folder_name} folder is not exist!")
    return True


def rem_folder(folder_name):
    if os.path.exists(folder_name):
        for filename in os.listdir(folder_name):
            file_path = os.path.join(folder_name, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))
    else:
        os.mkdir(folder_name)
        print(f"{folder_name} folder has created!")


def writer_smrfresult(inF, obj_points, file, out_dir=''):

    fname = file.split('/')[-1].split('.')[0] + '-filtered.las'
    print(fname)
    ground = ~obj_points
    withh = inF.withheld
    print("Withhled bits", set(withh))
    withh = np.subtract(1, np.array(withh)).astype(bool)
    keep_points = [a and b for a, b in zip(withh, ground)]
    out_las_data = laspy.LasData(inF.header)
    out_las_data.points = inF.points[keep_points]
    clasf = out_las_data.classification
    print(len(clasf))
    out_las_data.classification = [2 if x != 2 else 2 for x in clasf]
    print("Classification values", set(out_las_data.classification))

    with open_cl(out_dir + '/' + fname, 'wb') as f:
        out_las_data.write(f)

    return True


def writer_changes_inlasfile(inF, obj_points, file, out_bucket, out_dir='',reduce_stream=False):

    fname = file.split('/')[-1].split('.')[0] + '-filtered DEM.las'
    ground = ~obj_points 
    print(ground.sum())
    withh = inF.withheld
    print("Withhled bits", set(withh))
    # print(len(ground), len(withh))
    # print(ground)
    withh = np.subtract(1, np.array(withh)).astype(bool)
    s = time.time()
    clasf = inF.classification
    print(len(clasf[clasf==2]))
    clasf[ground] = 2
    clasf = clasf[withh]
    print(len(ground), len(clasf))
    # keep_points = np.logical_and(withh, ground).tolist()
    out_las_data = laspy.LasData(inF.header)
    out_las_data.points = inF.points[withh]
    out_las_data.classification = clasf
    print("Classification values", set(out_las_data.classification))

    with open_cl(out_dir + '/' + fname, 'wb') as f:
        out_las_data.write(f)
    
    # TODO check if (using Lithops' open before) the upload to bucket code can be deleted.
    with open_cl(out_dir + '/' + fname, 'rb') as f:
        data = f.read()

    print("Save points {}".format(time.time() - s))

    if not reduce_stream:
        data = None
        return fname
    else:
        return fname, data



def writer_lasfile(inF, obj_points, file, out_dir='',reduce_stream=False, is_bool = True):

    fname = file.split('/')[-1].split('.')[0] + '-filtered DEM.las'
    points = ~obj_points if is_bool else obj_points
    save_time = time.time()
    # print(obj_points.shape)
    if is_bool:
        print('is_bool')
        withh = inF.withheld
        print("Withhled bits", set(withh), "shape", withh.shape)
        withh = np.subtract(1, np.array(withh)).astype(bool) if len(points) == len(withh) else \
                np.subtract(1, np.array(withh[withh==0])).astype(bool)
        print("Withhled bits", set(withh), "shape", withh.shape)
    
        # keep_points = [a and b for a, b in zip(withh, ground)]
        keep_points = np.logical_and(withh, points).tolist()
        out_las_data = laspy.LasData(inF.header)
        out_las_data.points = inF.points[keep_points]
        out_las_data.classification = (np.ones(len(out_las_data.points)) * 2)
    else:
        print('isn\'t_bool')
        keep_points = points
        # print(type(keep_points))
        out_las_data = laspy.LasData(inF.header)
        out_las_data.points = inF.points[keep_points]


    print("Classification values", set(out_las_data.classification))
    with open_cl(out_dir + '/' + fname, 'wb') as f:
        out_las_data.write(f)
    
    # TODO check if (using Lithops' open before) the upload to bucket code can be deleted.
    with open_cl(out_dir + '/' + fname, 'rb') as f:
        data = f.read()

    print("Save points time {}".format(time.time() - save_time))

    if not reduce_stream:
        data = None
        # # Upload the tiled file
        # try:
        #     storage.put_object(out_bucket, fname, data)
        #     print('file %s has uploaded' % (fname))
        # except Exception as e:
        #     print('Failed to upload %s. Reason: %s' % (fname, e))
        return fname
    else:
        return fname, data


def outlier_lasfile(inF, points, file, out_bucket, reduce_stream=False):
    fname = file.split('/')[-1].split('.')[0] + '-removal_outlier.las'
    inlier = np.in1d(points, np.where(inF.points))

    out_las_data = laspy.LasData(inF.header)
    out_las_data.points = inF.points[inlier]

    with open(fname, 'wb') as f:
        out_las_data.write(f)

    # TODO check if (using Lithops' open before) the upload to bucket code can be deleted.
    with open(fname, 'rb') as f:
        data = f.read()

    if not reduce_stream:
        # Upload the tiled file
        try:
            storage.put_object(out_bucket, fname, data)
            print('file %s has uploaded' % (fname))
        except Exception as e:
            print('Failed to upload %s. Reason: %s' % (fname, e))
        data = None
        return fname
    else:
        return fname, data


def merg_results(in_bucket, out_bucket, num_part):
    # Start merging
    st_merg = time.time()
    mu_time = dict()
    storage = Storage()
    out_dir = 'downloaded_files'
    merged_file = 'merged_file'
    folders = [out_dir, merged_file]
    [rem_folder(folder) for folder in folders]

    # Download files from bucket
    st_dow = time.time()
    keys = storage.list_keys(in_bucket)
    for key in keys:
        data = storage.get_object(in_bucket, key)
        with open(os.path.join(out_dir, key), 'wb') as f:
            f.write(data)
    end_dow = time.time() - st_dow

    # Start merging 
    st_merging = time.time()
    files = os.listdir(out_dir)
    fname = files[0].split('.')[0].split('_')[0] + '-SMRF-' + str(num_part) + '-partitions.las'
    print(fname)

    files = [os.path.join(out_dir, file) for file in os.listdir(out_dir)]

    # Save files names in text file for merging process
    with open(os.path.join(os.getcwd(), 'lasfiles.txt'), 'w') as f:
        l1 = map(lambda x: x + '\n', files)
        f.writelines(l1)

    # Start merging process
    outfile = os.path.join(merged_file, fname)
    inpfiles = os.path.join(os.getcwd(), 'lasfiles.txt')

    LASInfo = 'lasmerge -lof {} -o {}'.format(shlex.quote(inpfiles), shlex.quote(outfile))
    LASInfo = shlex.split(LASInfo)

    print(LASInfo)
    try:
        pcs = subprocess.Popen(LASInfo)  # start this process
        pcs.wait()
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    # Re-order the resulted file according to the 'gps_time' column
    with open(os.path.join(merged_file, fname), 'wb') as inFile:
        in_las_data = laspy.read(inFile)
    laspoint = in_las_data.points
    np.ndarray.sort(laspoint["point"], kind='mergesort', order='gps_time')

    end_merging = time.time() - st_merging

    # Upload the tiled file
    st_up = time.time()
    with open(os.path.join(merged_file, fname), 'rb') as f:
        data = f.read()
    try:
        storage.put_object(out_bucket, fname, data)
        print('file %s has uploaded' % (fname))
    except Exception as e:
        print('Failed to upload %s. Reason: %s' % (fname, e))
    end_up = time.time() - st_up

    mu_time['File_name'] = fname
    mu_time['Points_number'] = len(laspoint)
    mu_time['Saving_data_time'] = round(end_dow, 3)
    mu_time['Merging_files_time'] = round(end_merging, 3)
    mu_time['Uploading_time'] = round(end_up, 3)
    mu_time['Merging_process'] = round(time.time() - st_merg, 3)

    return mu_time


def merg_streamres(res_mapdata, out_bucket, num_part):
    # Start merging
    st_merg = time.time()
    mu_time = dict()
    # storage = Storage()
    out_dir = 'downloaded_files'
    merged_file = 'merged_file'
    folders = [out_dir, merged_file]
    [rem_folder(folder) for folder in folders]

    # Download files from bucket
    # keys = storage.list_keys(in_bucket)
    for key in res_mapdata:
        data = key['data_stream']
        with open(os.path.join(out_dir, key['Object_name']), 'wb') as f:
            f.write(data)

    files = os.listdir(out_dir)
    fname = files[0].split('.')[0].split('_')[0] + '-SMRF-' + str(num_part) + '-partitions.las'

    with open(os.path.join(out_dir, files[0]), 'rb') as inF1:
        in_las_data1 = laspy.read(inF1)
    points = in_las_data1.points

    out_las_data = laspy.LasData(in_las_data1.header)

    if len(files) > 1:
        for i in range(1, len(files)):
            with open(os.path.join(out_dir, files[i]), 'rb') as inF_all:
                inf_las_data_all = laspy.read(inF_all)
            points = np.concatenate((points, inf_las_data_all.points))

            inf_las_data_all.close()

    out_las_data.points = points
    laspoint = out_las_data.points
    np.ndarray.sort(laspoint["point"], kind='mergesort', order='gps_time')
    with open(os.path.join(merged_file, fname), 'wb') as outFile:
        out_las_data.write(outFile)
    end_merg = time.time() - st_merg

    # Upload the tiled file
    st_up = time.time()
    with open(os.path.join(merged_file, fname), 'rb') as f:
        data = f.read()
    try:
        storage.put_object(out_bucket, fname, data)

    except Exception as e:
        print('Failed to upload %s. Reason: %s' % (fname, e))
    end_up = time.time() - st_up

    mu_time['File_name'] = fname
    mu_time['Points_number'] = len(points)
    mu_time['Merging_time'] = round(end_merg, 3)
    mu_time['Uploading_time'] = round(end_up, 3)

    return mu_time

def test(out):
    from lithops.storage.cloud_proxy import os
    try:
        print(os.path.isdir(out))
    except OSError as e:
        if e.errno == 12:
            print('OSError no. 12 caught')
        else:
            raise

def prox_merg_streamres(res_mapdata, num_part, buffer):

    # Start merging
    st_merg = time.time()
    mu_time = dict()
    out_dir = 'save_files'
    merged_file = 'merged_files'
    folders = [out_dir, merged_file]

    try:
        for folder in folders:
            rem_folder(out_dir)
    except Exception as e:
        print('Failed !! Reason: %s' % e)
        
    # define the resulted file name
    fname = res_mapdata[0]['Object_name'].split('.')[0].split('_')[0] + '-SMRF-' + str(num_part).zfill(3) + '-' + \
            str(buffer).zfill(2) + '-partitions.las'
    print(fname)

    # Save las files
    st_dow = time.time()
    for key in res_mapdata:
        data = key['data_stream']
        with open(out_dir + '/' + key['Object_name'], 'wb') as f:
            f.write(data)
    del res_mapdata
    files = [os.path.join(out_dir, file) for file in os.listdir(out_dir)]

    # Save files names in text file for merging process
    inpfiles = 'lasfiles.txt'
    with open(inpfiles, 'w') as f:
        l1 = map(lambda x: x + '\n', files)
        f.writelines(l1)
    end_dow = time.time() - st_dow


    # Start merging process
    st_merging = time.time()
    outfile = fname  #os.path.join(merged_file, fname)
    # inpfiles = 'lasfiles.txt' # os.path.join(os.getcwd(), 'lasfiles.txt')

    # LASInfo = 'lasmerge -h'# -i {} -o {}'.format(shlex.quote(out_dir + '/*.las'), shlex.quote(outfile))
    LASInfo = 'lasmerge -lof {} -o {}'.format(shlex.quote(inpfiles), shlex.quote(outfile))

    LASInfo = shlex.split(LASInfo)

    # print(LASInfo)
    try:
        print("Start merging all results")
        pcs = subprocess.Popen(LASInfo)  # start this process
        pcs.wait()
        print("Merging has finished")
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    # Re-order the resulted file according to the 'gps_time' column
    with open(fname, 'rb') as f:
        inFile = laspy.read(f)
        laspoint = inFile.points
    sorted_ind = np.argsort(laspoint.array['gps_time'])
    laspoints = laspoint[sorted_ind]
    # laspoint = np.ndarray.sort(laspoint["point"], kind='mergesort', order='gps_time')
    end_merging = time.time() - st_merging

    # Save it in the cloud
    st_up = time.time()
    out_las_data = laspy.LasData(inFile.header)
    out_las_data.points = laspoints
    with open_cl(merged_file + '/' + fname, 'wb') as f:
                    out_las_data.write(f)

    end_up = time.time() - st_up

    mu_time['File_name'] = fname
    mu_time['Points_number'] = len(inFile.classification[inFile.classification==2])
    mu_time['Saving_data_time'] = round(end_dow, 3)
    mu_time['Merging_files_time'] = round(end_merging, 3)
    mu_time['Merged_uploading_time'] = round(end_up, 3)
    mu_time['Merging_process'] = round(time.time() - st_merg, 3)

    return mu_time
    

def merg_files(res_mapdata, num_part, buffer):
    # Start merging
    st_merg = time.time()
    mu_time = dict()
    out_dir = 'save_files'
    merged_file = 'merged_files'
    folders = [out_dir, merged_file]

    try:
        for folder in folders:
            rem_folder(out_dir)
    except Exception as e:
        print('Failed !! Reason: %s' % e)
        
    # define the resulted file name
    fname = res_mapdata[0]['Object_name'].split('.')[0].split('_')[0] + '-SMRF-' + str(num_part).zfill(3) + '-' + \
            str(buffer).zfill(2) + '-partitions.las'
    print(fname)

    # Save las files
    st_dow = time.time()
    for key in res_mapdata:
        data = key['data_stream']
        with open(out_dir + '/' + key['Object_name'], 'wb') as f:
            f.write(data)
    del res_mapdata
    files = [os.path.join(out_dir, file) for file in os.listdir(out_dir)]

    # Save files names in text file for merging process
    inpfiles = 'lasfiles.txt'
    with open(inpfiles, 'w') as f:
        l1 = map(lambda x: x + '\n', files)
        f.writelines(l1)
    end_dow = time.time() - st_dow


    # Start merging process
    st_merging = time.time()
    outfile = fname  

    LASInfo = 'lasmerge -lof {} -o {}'.format(shlex.quote(inpfiles), shlex.quote(outfile))
    LASInfo = shlex.split(LASInfo)

    # print(LASInfo)
    try:
        print("Start merging all results")
        pcs = subprocess.Popen(LASInfo)  # start this process
        pcs.wait()
        print("Merging has finished")
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    # Re-order the resulted file according to the 'gps_time' column
    with open(fname, 'rb') as f:
        inFile = laspy.read(f)
        laspoint = inFile.points
    # laspoints = np.sort(laspoint.array, kind='mergesort', order='gps_time')
    sorted_ind = np.argsort(laspoint.array['gps_time'])
    laspoints = laspoint[sorted_ind]
    # print(laspoints.dtype)
    end_merging = time.time() - st_merging
    # print(laspoints)
    # Save it in the cloud
    st_up = time.time()
    out_las_data = laspy.LasData(inFile.header)
    out_las_data.points = laspoints
    with open_cl(merged_file + '/' + fname, 'wb') as f:
                    out_las_data.write(f)

    end_up = time.time() - st_up

    mu_time['File_name'] = fname
    mu_time['Points_number'] = len(laspoint)
    mu_time['Saving_data_time'] = round(end_dow, 3)
    mu_time['Merging_files_time'] = round(end_merging, 3)
    mu_time['Merged_uploading_time'] = round(end_up, 3)
    mu_time['Merging_process'] = round(time.time() - st_merg, 3)

    return mu_time

def byt_merg_streamres(res_mapdata, out_bucket, num_part):
    # Start merging
    st_merg = time.time()
    mu_time = dict()
    out_dir = 'downloaded_files'
    merged_file = 'merged_file'
    folders = [out_dir, merged_file]
    # [rem_folder(folder) for folder in folders]

    # define the resulted file name
    fname = res_mapdata[0]['Object_name'].split('.')[0].split('_')[0] + '-SMRF-' + str(num_part) + '-partitions.las'
    print(fname)

    # Save las files
    st_dow = time.time()
    for key in res_mapdata:
        data = key['data_stream']
        # with open(out_dir + '/' + key['Object_name'], 'wb') as f:
        #     key['data_stream'].write(f)
        with open(out_dir + '/' + key['Object_name'], 'wb') as f:
            f.write(data)
    del res_mapdata
    files = os.listdir(out_dir)
    # files = [os.path.join(out_dir, file) for file in os.listdir(out_dir)]

    # Save files names in text file for merging process
    inpfiles = 'lasfiles.txt'
    with open(inpfiles, 'w') as f:
        l1 = map(lambda x: x + '\n', files)
        f.writelines(l1)
    end_dow = time.time() - st_dow

    # Start merging process
    st_merging = time.time()
    outfile = merged_file + '/' + fname
    inpfiles = 'lasfiles.txt' # os.path.join(os.getcwd(), 'lasfiles.txt')

    LASInfo = 'lasmerge -lof {} -o {}'.format(shlex.quote(inpfiles), shlex.quote(outfile))
    LASInfo = shlex.split(LASInfo)

    print(LASInfo)
    try:
        pcs = subprocess.Popen(LASInfo)  # start this process
        pcs.wait()
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

    # Re-order the resulted file according to the 'gps_time' column
    with open(merged_file + '/' + fname, 'rw') as f:
        inFile = laspy.read(f)
        # inFile = laspy.file.File(os.path.join(merged_file, fname), mode='rw')
        laspoint = inFile.points
        np.ndarray.sort(laspoint["point"], kind='mergesort', order='gps_time')
        # inFile.close()
    end_merging = time.time() - st_merging

    # Upload the tiled file
    st_up = time.time()

    # with open(os.path.join(merged_file, fname), 'rb') as f:
    #     data = f.read()
    # try:
    #     storage.put_object(out_bucket, fname, data)

    # except Exception as e:
    #     print('Failed to upload %s. Reason: %s' % (fname, e))

    end_up = time.time() - st_up

    mu_time['File_name'] = fname
    mu_time['Points_number'] = len(laspoint)
    mu_time['Saving_data_time'] = round(end_dow, 3)
    mu_time['Merging_files_time'] = round(end_merging, 3)
    mu_time['Merged_uploading_time'] = round(end_up, 3)
    mu_time['Merging_process'] = round(time.time() - st_merg, 3)

    return mu_time


def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True):
    # Check if colors where provided, otherwhise use the default color cycle
    if colors is None:
        colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

    # Number of bars per group
    n_bars = len(data)

    # The width of a single bar
    bar_width = total_width / n_bars

    # List containing handles for the drawn bars, used for the legend
    bars = []

    # Iterate over all data
    for i, (name, values) in enumerate(data.items()):
        # The offset in x direction of that bar
        x_offset = (i - n_bars / 2) * bar_width + bar_width / 2

        # Draw a bar for every value of that type
        for x, y in enumerate(values):
            bar = ax.bar(x + x_offset, y, width=bar_width * single_width, color=colors[i % len(colors)])

        # Add a handle to the last drawn bar, which we'll need for the legend
        bars.append(bar[0])

    # Draw legend if we need
    if legend:
        ax.legend(bars, data.keys())
