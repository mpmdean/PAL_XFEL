from h5file import h5file
import os
import glob
import numpy as np

def get_f(run_no, scan_no, point_no, onoff, out_folder):
    filepath = os.path.join(out_folder, 'run={:03d}'.format(run_no),
                       'scan={:03d}'.format(scan_no),
                       'p{:04d}_{}.h5'.format(point_no, onoff))
    f = h5file(filepath)
    return f

def get_array(run_no, scan_no, key, onoff, out_folder):
    path = os.path.join(out_folder, 'run={:03d}'.format(run_no),
                       'scan={:03d}/*.h5'.format(scan_no))
    points = np.unique([int(p.split('/p')[-1][:4]) for p in glob.glob(path)])
    
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
    points = np.unique([int(p.split('/p')[-1][:4]) for p in glob.glob(path)])
    
    shots = np.array([get_f(run_no, scan_no, point_no, onoff, out_folder)['timestamp'].size
                      for point_no in points])
    return shots