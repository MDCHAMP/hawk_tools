
from typing import List, Union, Optional


import h5py
import warnings
import numpy as np


def parse_lms_tree(hf_obj):
    def read_dataset_val(ds):
        if len(ds.maxshape) == 0:
            if ds.dtype == 'O':
                v = hf_obj[()].decode()
            else:
                v = hf_obj[()]
        else:
            v = np.empty(ds.maxshape, dtype=ds.dtype)
            hf_obj.read_direct(v)
        return v
        
    out = {}
    nn = hf_obj.name.split('/')[-1]
    if isinstance(hf_obj, h5py.Dataset):
        if 'units' in hf_obj.attrs:
            out[nn] = {
                'value': read_dataset_val(hf_obj),
                'units': hf_obj.attrs.get('units')
            }
        else:
            out[nn] = read_dataset_val(hf_obj)
    elif isinstance(hf_obj, h5py.Group):
        out[nn] = {}
        for k in hf_obj.keys():
            out[nn] |= parse_lms_tree(hf_obj[k])
    return out


def load_hdf5(
    filename: str,
    sensors: Optional[Union[str,List[str]]] = None,
    data: Optional[Union[str, List[str]]] = None,
    meta: Optional[bool] = True,
    attrs: Optional[bool] = True,
    compress_x_axis: Optional[bool] = False,
    test_series: Optional[str] = 'LMS'
):



    if filename.split('/')[-1].startswith('LMS'):
        test_series = 'LMS'
    elif filename.split('/')[-1].startswith('NI'):
        test_series = 'NI'

    if type(sensors) == str:
        sensors = [sensors]
    
    if type(data) == str:
        data = [data]

    hf = h5py.File(filename, 'r')

    out = {}
    
    if test_series == 'LMS':
        lms_attr_fields = [
            "assettNumber",
            "calibration",
            "location",
            "model",
            "sensor"
        ]

        out = {}
        for k in hf.keys():
            if k == 'Meta' and meta:
                out |= parse_lms_tree(hf['Meta'])
            elif sensors is None or k in sensors:
                out[k] = {}
                for d in hf[k].keys():
                    if attrs and d in lms_attr_fields:
                        out[k].update(parse_lms_tree(hf[k][d]))
                    elif data is None or d in data:
                        if compress_x_axis:
                            if 'X_data' not in out.keys(): 
                                out['X_data'] = {}
                            if d not in out['X_data'].keys():
                                out['X_data'][d] = {
                                    'value': np.empty(hf[k][d]['X_data'].maxshape, dtype=hf[k][d]['X_data'].dtype),
                                    'units': hf[k][d]['X_data'].attrs.get('units')
                                }
                                hf[k][d]['X_data'].read_direct(out['X_data'][d]['value'])
                            out[k][d]= {'Y_data' : {
                                'value': np.empty(hf[k][d]['Y_data'].maxshape, dtype=hf[k][d]['Y_data'].dtype),
                                'units': hf[k][d]['Y_data'].attrs.get('units')
                                }
                            }
                            hf[k][d]['Y_data'].read_direct(out[k][d]['Y_data']['value'])
                        else:
                            out[k][d] = {'X_data': {
                                    'value': np.empty(hf[k][d]['X_data'].maxshape, dtype=hf[k][d]['X_data'].dtype),
                                    'units': hf[k][d]['Y_data'].attrs.get('units')
                                    },
                                    'Y_data': {
                                    'value': np.empty(hf[k][d]['Y_data'].maxshape, dtype=hf[k][d]['Y_data']),
                                    'units': hf[k][d]['Y_data'].attrs.get('units')
                                    }}
                            hf[k][d]['X_data'].read_direct(out[k][d]['X_data']['value'])
                            hf[k][d]['Y_data'].read_direct(out[k][d]['Y_data']['value'])
                    
        
    elif test_series == 'NI':
        if data is not None:
            warnings.warn('Ignoring data field: NI Data will always load all available data, i.e. only the time series measurement is available and is always loaded.')
        out = {}
        for k in hf.keys():
            if k == 'meta' and meta:
                out['Meta'] = {k:hf['meta'].attrs.get(k) for k in hf['meta'].attrs}
            elif sensors is None or k in sensors:
                if compress_x_axis:
                    if 'Timestamp' not in out.keys():
                        out['Timestamp'] = {
                            "value": np.empty(hf[k]['Timestamp'].maxshape, dtype=hf[k]['Timestamp'].dtype),
                            "units": "s"
                        }
                        hf[k]['Timestamp'].read_direct(out['Timestamp']['value'])
                    out[k] = {
                        'Measurement': {
                            "value": np.empty(hf[k]['Measurement'].maxshape, dtype=hf[k]['Measurement'].dtype),
                            "units": hf[k]['Measurement'].parent.attrs.get('Units')
                        }
                    }
                    hf[k]['Measurement'].read_direct(out[k]['Measurement']['value'])
                else:
                    out[k] = {
                        'Measurement': {
                            "value": np.empty(hf[k]['Measurement'].maxshape, dtype=hf[k]['Measurement'].dtype),
                            "units": hf[k]['Measurement'].parent.attrs.get('Units')
                        },
                        'Timestamp':  {
                            "value": np.empty(hf[k]['Timestamp'].maxshape, dtype=hf[k]['Timestamp'].dtype),
                            "units": "s"
                        }
                    }
                    hf[k]['Measurement'].read_direct(out[k]['Measurement']['value'])
                    hf[k]['Timestamp'].read_direct(out[k]['Timestamp']['value'])
    else:
        raise ValueError('Invalid HDF5 Test Series')

    return out