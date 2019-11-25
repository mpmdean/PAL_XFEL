from h5file import h5file
import os
from glob import glob
import numpy as np
from PAL_XFEL.compress import get_points


#def get_f(run_no, scan_no, point_no, onoff, out_folder):
#    filepath = os.path.join(out_folder, 'run={:03d}'.format(run_no),
#                       'scan={:03d}'.format(scan_no),
#                       'p{:04d}_{}.h5'.format(point_no, onoff))
#    try:
#        f = h5file(filepath)
#    except:
#        print("{} not found".format(filepath))
#        f = None
#    return f
#
def get_array(run_no, scan_no, key, onoff, out_folder):
    path = os.path.join(out_folder, 'run={:03d}'.format(run_no),
                       'scan={:03d}/*.h5'.format(scan_no))
    points = np.unique([int(p.split('/p')[-1][:4]) for p in glob(path)])
    
    array = np.array([get_f(run_no, scan_no, point_no, onoff, out_folder)[key][()]
                      for point_no in points])
    return array

def get_arrays(run_no, scan_nos, key, onoff, out_folder):
    array = np.concatenate([get_array(run_no, scan_no, key, onoff, out_folder)
                          for scan_no in scan_nos])
    return array

def get_shots(run_no, scan_no, key, onoff, out_folder):
    path = os.path.join(out_folder, 'run={:03d}'.format(run_no),
                       'scan={:03d}/*.h5'.format(scan_no))
    points = np.unique([int(p.split('/p')[-1][:4]) for p in glob(path)])
    
    shots = np.array([get_f(run_no, scan_no, point_no, onoff, out_folder)['timestamp'].size
                      for point_no in points])
    return shots


def get_points_produced(out_folder, run_no, scan_no=1):
    out_path = os.path.join(out_folder, 'run={:03d}'.format(run_no), 'scan={:03d}'.format(scan_no), '*.h5')
    points = [int(path.split('/')[-1][1:5]) for path in glob(out_path)]
    return np.unique(points)

def processed_status(out_folder, folder_stem_images, run_no, scan_no=1):
    points_ready = get_points(folder_stem_images, run_no, scan_no=scan_no)
    points_produced = get_points_produced(out_folder, run_no, scan_no=scan_no)
    not_done = set(points_ready) - set(points_produced)
    print('{} points produced\n{} not processed'.format(len(points_produced), not_done))
    return not_done, points_ready, points_produced


def get_f(run_no, scan_no, point_no, onoff, out_folder, verbose=True):
    filepath = os.path.join(out_folder, 'run={:03d}'.format(run_no),
                       'scan={:03d}'.format(scan_no),
                       'p{:04d}_{}.h5'.format(point_no, onoff))
    try:
        f = h5file(filepath)
    except:
        if verbose:
            print("{} not found".format(filepath))
        f = None
    return f


def get_array_points(run_no, points, scan_no, key, onoff, out_folder):
    path = os.path.join(out_folder, 'run={:03d}'.format(run_no),
                       'scan={:03d}/*.h5'.format(scan_no),
                       verbose=True)
    datapoints = []
    for point_no in points:
        try:
            datapoints.append(get_f(run_no, scan_no, point_no,
                                    onoff, out_folder, verbose=verbose)[key][()])
        except:
            datapoints.append(None)
    
    if any([datapoint is None for datapoint in datapoints]):
        for datapoint in datapoints:
            try:
                NaNdatapoint = np.full_like(datapoint, np.NaN)
            except TypeError:
                pass

        for i in range(len(datapoints)):
            if datapoints[i] is None:
                if verbose:
                    print("filling NaN for missing data run={} scan={} point={}".format(run_no, scan_no, points[i]))
                datapoints[i] = NaNdatapoint
            
    return np.array(datapoints)


def get_f(run_no, scan_no, point_no, onoff, out_folder, verbose=True):
    filepath = os.path.join(out_folder, 'run={:03d}'.format(run_no),
                       'scan={:03d}'.format(scan_no),
                       'p{:04d}_{}.h5'.format(point_no, onoff))
    try:
        f = h5file(filepath)
    except:
        if verbose:
            print("{} not found".format(filepath))
        f = None
    return f

def get_arrays_points(run_no, points, scan_nos, key, onoff, out_folder, verbose=True):
    array = np.array([get_array_points(run_no, points, scan_no, key, onoff, out_folder, verbose=verbose)
                          for scan_no in scan_nos])
    return np.nanmean(array, axis=0)

def get_array_points(run_no, points, scan_no, key, onoff, out_folder,
                       verbose=True):
    path = os.path.join(out_folder, 'run={:03d}'.format(run_no),
                       'scan={:03d}/*.h5'.format(scan_no))
    datapoints = []
    for point_no in points:
        try:
            datapoints.append(get_f(run_no, scan_no, point_no, onoff, out_folder, verbose=verbose)[key][()])
        except:
            datapoints.append(None)
    
    if any([datapoint is None for datapoint in datapoints]):
        for datapoint in datapoints:
            try:
                NaNdatapoint = np.full_like(datapoint, np.NaN)
            except TypeError:
                pass

        for i in range(len(datapoints)):
            if datapoints[i] is None:
                if verbose:
                    print("filling NaN for missing data run={} scan={} point={}".format(run_no, scan_no, points[i]))
                datapoints[i] = NaNdatapoint
            
    return np.array(datapoints)