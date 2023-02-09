"""
This function takes the HDF5 file specified by "filename" and loads the data into 
a python dictionary. The keys of this dictionary represent a single sensor and 
contains both sensor information such as location and calibration information and 
the sensor readings taken from the LMS data acquisition system. In addition to 
the sensor information, there is a "Meta" key that contains the testing parameters 
such as excitation, added mass, and any notes taken by the testing engineer.

This loader is tested for the DTHIVE starboard wing modal testing performed at 
the Laboratory for Verification and Validation at the University of Sheffield.

This function can either be imported and called within a custom script or can be 
run directly and load the default value defined in the function definition. The 
output of the function is a dictionary variable "Data".


Created by:
    Dr. Matthew Bonney - The University of Sheffield
Created on:
    24/11/2022
    
Last Updated on:
    

"""
import h5py

# def load_hdf5(filename: str, meta: bool = True, )


def load_hdf5(filename: str = "BR_AR_1_1.hdf5"):
    """
    This function loads the HDF5 file and populates a python dictionary containing 
    all the information stored. The structure is set up as each group is a new 
    layer in the dictionary. For each dataset, if there is associated metadata, 
    the entry is split into a new dictionary with the dataset under the key "value" 
    and the metadata is set as the other keys within the dataset dictionary.

    Parameters
    ----------
    filename : str, optional
        This is the path to the desired hdf5 file. The default is "BR_AR_1_1.hdf5".
        Note that if the files are nested within folders, it is recomended to 
        use the command "os.path.join()" to be operating system independent. 

    Returns
    -------
    Data : dict
        A dictionary containing the stored data. The dictionary keys are the sensor
        names and a Meta entry for testing parameters.

    Example
    -------
    from hdf5_loader import load_hdf5
    Rep_1 = load_hdf5("BR_AR_1_1.hdf5")
    Rep_2 = load_hdf5("BR_AR_1_2.hdf5")
    Rep_3 = load_hdf5("BR_AR_1_3.hdf5")

    """
    # Open hdf5 file
    hf = h5py.File(filename, 'r')
    # Create list of entries
    lis = []
    def printer(x): return lis.append(x)
    hf.visit(printer)
    # Initialise variable
    Data = {}
    # Loop through entries
    for name in lis:
        if type(hf[name]) == h5py._hl.group.Group:
            # Create Dictionary
            le = name.split('/')
            temp = Data
            for i in range(len(le)):  # Nested Dictonaries
                if not le[i] in temp:  # Nested dictionary does not exist
                    temp[le[i]] = {}
                # Recursive loop for any level of nested loops
                temp = temp[le[i]]
        elif type(hf[name]) == h5py._hl.dataset.Dataset:
            value = hf[name][()]
            # Loop into nested loop
            le = name.split('/')
            temp = Data
            for i in range(len(le)-1):  # Get to innermost nest
                temp = temp[le[i]]
            # Gather Metadata of dataset
            attr = hf[name].attrs
            # Parse data into dictionary
            if len(attr) == 0:  # No Metadata
                temp[le[-1]] = value
            else:  # If Metadata, create a dictionary of value and metadata
                temp[le[-1]] = {}
                temp = temp[le[-1]]
                temp['value'] = value  # Store main value
                for keys in hf[name].attrs:
                    val = hf[name].attrs.get(keys)
                    temp[keys] = val  # Store metadata
    hf.close()  # close file
    return (Data)


if __name__ == "__main__":
    Data = load_hdf5()
