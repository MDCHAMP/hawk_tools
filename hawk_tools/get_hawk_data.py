import collections
import os

import gdown
import six

from hawk_tools.lut import keys as data_ids
from hawk_tools.new_hdf5_loader import load_hdf5


def isiter(arg):
    return isinstance(arg, collections.abc.Iterable) and not isinstance(
        arg, six.string_types
    )

def get_data_from_test_key(camp, key, tmpdir):
    if not os.path.exists(tmpdir):
        os.makedirs(tmpdir)
    fname = os.path.join(tmpdir, f"{camp}_{key}.hd5")
    if not os.path.isfile(fname):
        gdown.download(id=data_ids[camp][key], output=fname, quiet=False)
    else:
        print(f"Using cached version: {fname}")
    dat = load_hdf5(fname)
    return dat


def get_hawk_data(
    test_camp, test_id, test_runs=None, test_reps=None, download_dir="./.hawk_data", disk_only=False, ask=True
):
    if not test_camp in {"NI", "LMS"}:
        raise ValueError(
            f"Please select LMS or NI for frequency and time datasets repsctively"
        )
    keys = []
    all_runs = set([
        int(k.split("_")[-2]) for k in data_ids[test_camp].keys() if test_id in k
    ])
    if test_runs is None:
        test_runs = all_runs
    elif not isiter(test_runs):
        test_runs = [test_runs]
    for run in test_runs:
        all_reps = set([
            int(k.split("_")[-1])
            for k in data_ids[test_camp].keys()
            if f"{test_id}_{run}" in k
        ])
        if test_reps is None:
            test_reps = all_reps
        elif not isiter(test_reps):
            test_reps = [test_reps]
        for rep in test_reps:
            keys.append(f"{test_id}_{run}_{rep}")
    keys = set(keys)
    if len(keys) >= 10 and ask:
        flag = input(
            f"A large number of data files have been requested ({len(keys)}) i.e. >{len(keys)*0.2}GB of data do you wish to continue? y/n   "
        )
        if flag not in {"y", "Y", "yes"}:
            return
    d = {}
    for k in keys:
        if disk_only:
            _ = get_data_from_test_key(test_camp, k, download_dir)
        else:
            d |= {k: get_data_from_test_key(test_camp, k, download_dir)}
    return d


# %%

#data = get_hawk_data("LMS", "BR_AR")
