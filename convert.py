# Written by Karl Zylinski (karl@zylinski.se)

# Input-data can be downloaded using torrent supplied next to this file, input-data.torrent.
# The data should be placed inside input-data folder and extracted so that the paths look like:
# input-data/000/b0000.cat, input-data/000/b0001.cat etc.
# The format of the input-data is described by input-data-format.html or
# http://tdc-www.harvard.edu/catalogs/ub1.format.html

# If you just want some data to test with without downloading torrent, then use the supplied sample.cat,
# place it as input-data/000/b0000.cat (sample.cat is a copy of that datafile from the torrent).

import os
import struct
import math

columns = ["J2000 ra", "J2000 dec",
           "pm ra", "pm dec", "pm prob", "pm catalog correction",
           "pm ra sigma", "pm dec sigma", "pm ra fit sigma", "pm dec fit sigma", "num detections"]
units = ["deg", "deg",
         "mas/year", "mas/year", "0.1", "0 or 1",
         "mas/year", "mas/year", "0.1 arcsec", "0.1 arcsec", "count"]
descriptions = ["Right ascension at J2000",
                "Declination at J2000",
                "Proper motion in RA, see 'pm catalog flag'"
                "Proper motion in DEC, see 'pm catalog flag'"
                "Objects with large proper motions were cross-correlated with the LHS (Luyten 1979a), the NLTT (Luyten 1979b), and the Lowell Proper Motion Survey (Giclas et al. 1971, Giclas et al. 1978). This flag is set if the USNO-B detection has a correlation in any of these catalog, but the data presented are from the PMM and not from the proper motion catalog."]
file_out = open("output.csv", mode="w")
file_in_path = "input-data/000/b0000.cat"
file_in = open(file_in_path, mode="rb")
file_in_size = os.path.getsize(file_in_path)
row_length = 80

# Gets field from packed int. For example we might have 4718914587 where
# different parts of the int measure different things.
def get_packed_field(packed_field, packed_field_exp_len, start, field_len):
    packed_field_s = str(packed_field)
    packed_field_len = len(packed_field_s)
    len_diff = packed_field_exp_len - packed_field_len
    start_corr = start - len_diff
    extracted = packed_field_s[start_corr:start_corr+field_len]
    return 0 if extracted == '' else int(extracted)

#if size % rowlength != 0:

    #continue

file_out.write(str.join(",", columns) + "\n")

while file_in.tell() != file_in_size:
    # Indices to variable raw_fields refer to the different packed ints described in format (see comment at top). I process the fields in exactly the same order as in the format and
    # use quantas and ranges specified there to extract values.
    raw_fields = struct.unpack('%di' % 20, file_in.read(80))

    out_fields = []
    ra = raw_fields[0] / 100 / 60 / 60
    out_fields.append("%.6f" % ra)

    # File uses south polar distance, subtract 90 deg
    dec = raw_fields[1] / 100 / 60 / 60 - 90
    out_fields.append("%.6f" % dec)
    
    rf2 = raw_fields[2]
    pm_ra = get_packed_field(rf2, 10, 6, 4) * 2 - 10000
    out_fields.append("%i" % pm_ra)
    pm_dec = get_packed_field(rf2, 10, 2, 4) * 2 - 10000
    out_fields.append("%i" % pm_dec)
    pm_prob = get_packed_field(rf2, 10, 1, 1)
    out_fields.append("%i" % pm_prob)
    pm_catalog_correction_flag = get_packed_field(rf2, 10, 0, 1)
    out_fields.append("%i" % pm_catalog_correction_flag)

    rf3 = raw_fields[3]
    pm_ra_sigma = get_packed_field(rf3, 10, 7, 3)
    out_fields.append("%i" % pm_ra_sigma)
    pm_dec_sigma = get_packed_field(rf3, 10, 4, 3)
    out_fields.append("%i" % pm_dec_sigma)
    pm_ra_fit_sigma = get_packed_field(rf3, 10, 3, 1)
    out_fields.append("%i" % pm_ra_fit_sigma)
    pm_dec_fit_sigma = get_packed_field(rf3, 10, 2, 1)
    out_fields.append("%i" % pm_dec_fit_sigma)
    num_detections = get_packed_field(rf3, 10, 1, 1)
    out_fields.append("%i" % num_detections)

    file_out.write(str.join(",", out_fields) + "\n")

file_in.close()
file_out.close()