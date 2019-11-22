import numpy as np
import dask.array as da
import pandas as pd
import os
from glob import glob
import h5py
from h5file import h5file


#==============================================================================
# Defining Global variables
#==============================================================================

# Motor nickname dict
motorKey = {
'delay': 'delay_input',
'th': 'th_input', 
'laserStatus': 'event_info.FIFTEEN_HERTZ',
'laser_h': 'laser_h_input',
'laser_v': 'laser_v_input'
}

# All the scalars needed

# qbpm and pd readings
qbpm = {
#'pink-ch1': 'qbpm:oh:qbpm1:ch1',
#'pink-ch2': 'qbpm:oh:qbpm1:ch2',
#'pink-ch3': 'qbpm:oh:qbpm1:ch3',
#'pink-ch4': 'qbpm:oh:qbpm1:ch4',
#'pink-sum': 'qbpm:oh:qbpm1:sum',
'mono-up-ch1': 'qbpm:oh:qbpm2:ch1',
'mono-up-ch2': 'qbpm:oh:qbpm2:ch2',
'mono-up-ch3': 'qbpm:oh:qbpm2:ch3',
'mono-up-ch4': 'qbpm:oh:qbpm2:ch4',
'mono-up-sum': 'qbpm:oh:qbpm2:sum',
#'sample-ch1': 'qbpm:eh1:qbpm1:ch1',
#'sample-ch2': 'qbpm:eh1:qbpm1:ch2',
#'sample-ch3': 'qbpm:eh1:qbpm1:ch3',
#'sample-ch4': 'qbpm:eh1:qbpm1:ch4',
'sample-sum': 'qbpm:eh1:qbpm1:sum',
'pd1-ch1': 'pd:es:pd1:ch1',
'pd1-ch2': 'pd:es:pd1:ch2',
'pd1-ch3': 'pd:es:pd1:ch3',
'pd1-ch4': 'pd:es:pd1:ch4'
}

# beam positions
qbpm_pos = {
#'pink-posx': 'qbpm:oh:qbpm1:pos_X',
#'pink-posy': 'qbpm:oh:qbpm1:pos_Y',
'mono-up-posx': 'qbpm:oh:qbpm2:pos_X',
'mono-up-posy': 'qbpm:oh:qbpm2:pos_Y'#,
#'sample-posx': 'qbpm:eh1:qbpm1:pos_X',
#'sample-posy': 'qbpm:eh1:qbpm1:pos_Y'
}

# MPCCD readings
mpccd = {
'full-max': 'detector:eh1:mpccd1:frame.max', 
'full-min': 'detector:eh1:mpccd1:frame.min',
'full-mean': 'detector:eh1:mpccd1:frame.mean',
'full-std': 'detector:eh1:mpccd1:frame.std',
'roi1-max': 'detector:eh1:mpccd1:ROI1_stat.max',
'roi1-min': 'detector:eh1:mpccd1:ROI1_stat.min',
'roi1-mean': 'detector:eh1:mpccd1:ROI1_stat.mean',
'roi1-std': 'detector:eh1:mpccd1:ROI1_stat.std',
'roi1-sum': 'detector:eh1:mpccd1:ROI1_stat.sum'
}

# Peak positions on the MPCCD - not critical unless drifts
mpccd_pos = {
'roi1-comy': 'detector:eh1:mpccd1:ROI1_stat.center_of_mass.y',
'roi1-comx': 'detector:eh1:mpccd1:ROI1_stat.center_of_mass.x'
}

all_keys = {**motorKey, **qbpm, **mpccd, **mpccd_pos}


#########################################################
folder_stem_images = '/xfel/ffs/dat/ue_191117_FXS/raw_data/h5/type=raw/run={:03d}/scan={:03d}/'
file_stem_scalars = '/xfel/ffs/dat/ue_191117_FXS/raw_data/h5/type=measurement/run={:03d}/scan={:03d}/p{:04d}.h5'

run_no = 13
scan_no = 1

def get_points(folder_stem_images, run_no, scan_no=1):
    folder_images = folder_stem_images.format(run_no, scan_no)
    points = [int(path[-7:-3]) for path in glob(os.path.join(folder_images, '*.h5'))]
    points.sort()
    return points

def read_data(run_no, point_no, scan_no=1,
              folder_stem_images=folder_stem_images,
              file_stem_scalars=file_stem_scalars):

    f_images = h5file(os.path.join(folder_stem_images.format(run_no, scan_no) + 'p{:04d}.h5'.format(point_no)))
    df_scalars_all = pd.read_hdf(file_stem_scalars.format(run_no, scan_no, point_no))
    tags = f_images['mpccd1/image/axis1'][()]

    df_scalars = df_scalars_all.loc[tags]

    choose_laser_on = df_scalars[motorKey['laserStatus']] == True
    df_on_full_names = df_scalars[choose_laser_on]
    choose_laser_off = df_scalars[motorKey['laserStatus']] == False
    df_off_full_names = df_scalars[choose_laser_off]

    df_on = df_on_full_names.rename(columns={v:k for k, v in all_keys.items()})
    get = [key for key in all_keys.keys() if key in df_on.keys()]
    df_on = df_on[get]
    df_off = df_off_full_names.rename(columns={v:k for k, v in all_keys.items()})
    df_off = df_off[get]

    # images = da.from_array(f_images['detector/eh1/mpccd1/image/block0_values'])
    # image_on, image_off = (
    #     da.compute(images[choose_laser_on.values, : :].mean(0),
    #               images[choose_laser_off.values, : :].mean(0))
    #)
    images = f_images['detector/eh1/mpccd1/image/block0_values']
    image_on = images[choose_laser_on.values, :, :].mean(0)
    image_off = images[choose_laser_off.values, :, :].mean(0)
    
    return df_on, df_off, image_on, image_off

out_folder = '/xfel/ffs/dat/ue_191123_FXS/reduced'

def write_data(run_no, point_no, 
               df_on, df_off, image_on, image_off,
              out_folder=out_folder, scan_no=1):
    path = os.path.join(out_folder, 'run={:03d}'.format(run_no),
                       'scan={:03d}'.format(scan_no))
    if not os.path.exists(path):
        os.makedirs(path)
    
    path_on = os.path.join(out_folder, 'run={:03d}'.format(run_no),
                           'scan={:03d}'.format(scan_no),
                           'p{:04d}_on.h5'.format(point_no))
    if os.path.exists(path_on):
        os.remove(path_on)
    f_on = h5py.File(path_on)
    
    path_off = os.path.join(out_folder, 'run={:03d}'.format(run_no),
                           'scan={:03d}'.format(scan_no),
                           'p{:04d}_off.h5'.format(point_no))
    if os.path.exists(path_off):
        os.remove(path_off)
    f_off = h5py.File(path_off)
    
    for df, image, f in zip([df_on, df_off], [image_on, image_off], [f_on, f_off]):
        for key in df.keys():
            if key in list(qbpm.keys()) + list(mpccd.keys()):
                val = np.nansum(df[key])
                f.create_dataset('p{:04d}_{}'.format(point_no, key), data=val)
            if key in list(qbpm_pos.keys()) + list(mpccd_pos.keys()) + list(motorKey.keys()):
                val = np.nanmean(df[key])
                f.create_dataset('p{:04d}_{}'.format(point_no, key), data=val)
        try:
            del f['p{:04d}_image'.format(point_no)]
        except:
            pass
        f.create_dataset('p{:04d}_image'.format(point_no), data=image)
        f.create_dataset('p{:04d}_timestamp'.format(point_no), data=df.index.values)
        print("Wrote {}".format(f.filename))
        f.close()
        
def read_write_run(run_no, scan_no=1,
              folder_stem_images=folder_stem_images,
              file_stem_scalars=file_stem_scalars,
               out_folder=out_folder, points=None):
    
    path_on = os.path.join(out_folder, 'run={:03d}_scan={:03d}_on.h5'.format(run_no, scan_no))
    f_on = h5py.File(path_on, 'a', libver='latest')
    path_off = os.path.join(out_folder, 'run={:03d}_scan={:03d}_off.h5'.format(run_no, scan_no))
    f_off = h5py.File(path_off, 'a', libver='latest')
    
    if points is None:
        points = get_points(folder_stem_images, run_no, scan_no=scan_no)
    for point_no in points:
        print("point {}".format(point_no))
        df_on, df_off, image_on, image_off = read_data(run_no, point_no, scan_no=scan_no,
                                    folder_stem_images=folder_stem_images,
                                    file_stem_scalars=file_stem_scalars)
        
        write_data(run_no, point_no, 
               df_on, df_off, image_on, image_off,
              out_folder=out_folder, scan_no=1)

