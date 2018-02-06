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
import datetime

# Useful configuration parameters
max_size_per_output_file = 52428800 # bytes.
start_zone_number = 0
end_zone_number = 400

columns = [ "usno_b1_id",
            "j2000_ra",
            "j2000_dec",
            "pm_ra",
            "pm_dec",
            "pm_prob",
            "pm_catalog_correction_flag",
            "pm_ra_sigma",
            "pm_dec_sigma",
            "ra_fit_sigma",
            "dec_fit_sigma",
            "num_detections",
            "diffraction_spike_flag",
            "j2000_ra_sigma",
            "j2000_dec_sigma",
            "epoch",
            "ys4_correlation_flag",
            "blue_1_mag",
            "blue_1_field",
            "blue_1_survey",
            "blue_1_galaxy_star_sep",
            "red_1_mag",
            "red_1_field",
            "red_1_survey",
            "red_1_galaxy_star_sep",
            "blue_2_mag",
            "blue_2_field",
            "blue_2_survey",
            "blue_2_galaxy_star_sep",
            "red_2_mag",
            "red_2_field",
            "red_2_survey",
            "red_2_galaxy_star_sep",
            "ir_mag",
            "ir_field",
            "ir_survey",
            "ir_galaxy_star_sep",
            "blue_1_xi_res",
            "blue_1_eta_res",
            "blue_1_calibration_source",
            "red_1_xi_res",
            "red_1_eta_res",
            "red_1_calibration_source",
            "blue_2_xi_res",
            "blue_2_eta_res",
            "blue_2_calibration_source",
            "red_2_xi_res",
            "red_2_eta_res",
            "red_2_calibration_source",
            "ir_xi_res",
            "ir_eta_res",
            "ir_calibration_source",
            "blue_1_scan_lookback_index",
            "red_1_scan_lookback_index",
            "blue_2_scan_lookback_index",
            "red_2_scan_lookback_index",
            "ir_scan_lookback_index" ]

# Gets field from packed int. For example we might have 4718914587 where
# we want to get a part of this number. packed_field_exp_len is passed
# because fields should always be of length in format, so pad with zeros.
def get_packed(packed_field, packed_field_exp_len, start, field_len):
    packed_field_padded = str(packed_field).zfill(packed_field_exp_len)
    extracted = packed_field_padded[start:start+field_len]
    return 0 if extracted == '' else int(extracted)

output_folder = datetime.datetime.now().strftime("output-%Y-%m-%d_%H-%M")

if os.path.isdir(output_folder) == False:
    os.mkdir(output_folder)

class OutputFile:
    index = -1
    handle = None

# Used when output file hits size max_size_per_output_file
def start_new_output_file(current_file):
    if current_file != None:
        current_file.handle.close()
    num = 0 if current_file == None else current_file.index + 1
    new_name = "%s/usno-b1-%i.csv"%(output_folder, num)
    print("Starting new output file: " + new_name)
    handle = open(new_name, mode="w")
    handle.write(str.join(",", columns) + "\n")
    output_file = OutputFile()
    output_file.index = num
    output_file.handle = handle
    return output_file

file_out = start_new_output_file(None)
prev_folder = ""

for zone_number in range(start_zone_number, end_zone_number + 1):
    # If one wants the script to follow max_size_per_output_file more precisely,
    # move following two lines inside the while-loop that processes each cat-file.
    if file_out.handle.tell() >= max_size_per_output_file:
        file_out = start_new_output_file(file_out)

    folder_name = ("%04i" % zone_number)[0:3]

    if prev_folder != folder_name:
        print("Processing folder " + folder_name)

    prev_folder = folder_name
    file_in_path = "input-data/%s/b%04i.cat" % (folder_name, zone_number)

    if os.path.isfile(file_in_path) == False:
        print("Aborted at " + file_in_path + ": File does not exist")
        break

    file_in = open(file_in_path, mode = "rb")
    file_in_size = os.path.getsize(file_in_path)
    row_length = 80

    if file_in_size % row_length != 0:
        print("Skipped file " + file_in_path + ": Size is not a multiple of row length")
        file_in.close()
        continue

    # This is the "second half" of the USNO B id, first is zone number. Ex: 0001-0000123 where 0001
    # is the zone number and 0000123 is the object counter. The object counter is reset for each zone.
    object_counter = 1

    # This processes a specific .cat-file, row for row
    while file_in.tell() != file_in_size:
        # Indices to variable raw_fields refer to the different packed ints described in format
        # (see comment at top). I process the fields in exactly the same order as in the format and
        # use quantas and ranges specified there to extract values. Any transformations other than
        # quanta/range adjustments are explicitly explained below.
        packed_ints_per_row = 20
        rfs = struct.unpack('%di' % packed_ints_per_row, file_in.read(row_length)) # RawFieldS

        def is_row_ok(row):
            for cell in row:
                if cell < 0:
                    return False

            return True

        usno_id = "%04i-%07i" % (zone_number, object_counter)

        if is_row_ok(rfs) == False:
            print("Skipped object %s at byte %i in file %s, failed sanity check." % (usno_id, file_in.tell() - row_length, file_in_path))
            continue

        out_fields = []

        def add(fmt, val, repeat=1):
            # Only do formatting if val is not zero, saves some space for floats
            formatted = "0" if val == 0 else fmt % val
            out_fields.extend([formatted]*repeat)

        # usno_b1_id
        add("%s", usno_id)

        # ra
        add("%.6f", rfs[0] / 100 / 60 / 60) 

        # dec -- Data uses south polar distance, subtract 90 deg
        add("%.6f", rfs[1] / 100 / 60 / 60 - 90)
        
        # pm_ra
        add("%i", get_packed(rfs[2], 10, 6, 4) * 2 - 10000)
        # pm_dec
        add("%i", get_packed(rfs[2], 10, 2, 4) * 2 - 10000)
        # pm_prob
        add("%i", get_packed(rfs[2], 10, 1, 1))
        # pm_catalog_correction_flag
        add("%i", get_packed(rfs[2], 10, 0, 1))

        # pm_ra_sigma 
        add("%i", get_packed(rfs[3], 10, 7, 3))
        # pm_dec_sigma
        add("%i", get_packed(rfs[3], 10, 4, 3))
        # ra_fit_sigma
        add("%i", get_packed(rfs[3], 10, 3, 1))
        # dec_fit_sigma
        add("%i", get_packed(rfs[3], 10, 2, 1))
        # num_detections
        add("%i", get_packed(rfs[3], 10, 1, 1))
        # diffraction_spike_flag
        add("%i", get_packed(rfs[3], 10, 0, 1))

        # ra_sigma
        add("%i", get_packed(rfs[4], 10, 7, 3))
        # dec_sigma
        add("%i", get_packed(rfs[4], 10, 4, 3))
        # epoch
        add("%.1f", get_packed(rfs[4], 10, 1, 3) / 10 + 1950)
        # ys4_correlation_flag 
        add("%i", get_packed(rfs[4], 10, 0, 1))


        # Remaining fields are said to be missing if the whole packed field is 0,
        # in that case we write ? to all those fields.

        if rfs[5] == 0:
            add("%s", "?", 4)
        else:
            # blue_1_mag
            add("%.2f", get_packed(rfs[5], 10, 6, 4) / 100)
            # blue_1_field
            add("%i", get_packed(rfs[5], 10, 3, 3))
            # blue_1_survey
            add("%i", get_packed(rfs[5], 10, 2, 1))
            # blue_1_galaxy_star_sep
            add("%i", get_packed(rfs[5], 10, 0, 2))

        if rfs[6] == 0:
            add("%s", "?", 4)
        else:
            # red_1_mag
            add("%.2f", get_packed(rfs[6], 10, 6, 4) / 100)
            # red_1_field
            add("%i", get_packed(rfs[6], 10, 3, 3))
            # red_1_survey
            add("%i", get_packed(rfs[6], 10, 2, 1))
            # red_1_galaxy_star_sep
            add("%i", get_packed(rfs[6], 10, 0, 2))

        if rfs[7] == 0:
            add("%s", "?", 4)
        else:
            # blue_2_mag
            add("%.2f", get_packed(rfs[7], 10, 6, 4) / 100)
            # blue_2_field
            add("%i", get_packed(rfs[7], 10, 3, 3))
            # blue_2_survey
            add("%i", get_packed(rfs[7], 10, 2, 1))
            # blue_2_galaxy_star_sep
            add("%i", get_packed(rfs[7], 10, 0, 2))

        if rfs[8] == 0:
            add("%s", "?", 4)
        else:
            # red_2_mag
            add("%.2f", get_packed(rfs[8], 10, 6, 4) / 100)
            # red_2_field
            add("%i", get_packed(rfs[8], 10, 3, 3))
            # red_2_survey
            add("%i", get_packed(rfs[8], 10, 2, 1))
            # red_2_galaxy_star_sep
            add("%i", get_packed(rfs[8], 10, 0, 2))

        if rfs[9] == 0:
            add("%s", "?", 4)
        else:
            # ir_mag
            add("%.2f", get_packed(rfs[9], 10, 6, 4) / 100)
            # ir_field
            add("%i", get_packed(rfs[9], 10, 3, 3))
            # ir_survey
            add("%i", get_packed(rfs[9], 10, 2, 1))
            # ir_galaxy_star_sep
            add("%i", get_packed(rfs[9], 10, 0, 2))

        if rfs[10] == 0:
            add("%s", "?", 3)
        else:
            # blue_1_xi_res
            add("%.2f", get_packed(rfs[10], 9, 5, 4) / 100 - 50)
            # blue_1_eta_res
            add("%.2f", get_packed(rfs[10], 9, 1, 4) / 100 - 50)
            # blue_1_calibration_source
            add("%i", get_packed(rfs[10], 9, 0, 1))

        if rfs[11] == 0:
            add("%s", "?", 3)
        else:
            # red_1_xi_res
            add("%.2f", get_packed(rfs[11], 9, 5, 4) / 100 - 50)
            # red_1_eta_res
            add("%.2f", get_packed(rfs[11], 9, 1, 4) / 100 - 50)
            # red_1_calibration_source
            add("%i", get_packed(rfs[11], 9, 0, 1))

        if rfs[12] == 0:
            add("%s", "?", 3)
        else:
            # blue_2_xi_res
            add("%.2f", get_packed(rfs[12], 9, 5, 4) / 100 - 50)
            # blue_2_eta_res
            add("%.2f", get_packed(rfs[12], 9, 1, 4) / 100 - 50)
            # blue_2_calibration_source
            add("%i", get_packed(rfs[12], 9, 0, 1))

        if rfs[13] == 0:
            add("%s", "?", 3)
        else:
            # red_2_xi_res
            add("%.2f", get_packed(rfs[13], 9, 5, 4) / 100 - 50)
            # red_2_eta_res
            add("%.2f", get_packed(rfs[13], 9, 1, 4) / 100 - 50)
            # red_2_calibration_source
            add("%i", get_packed(rfs[13], 9, 0, 1))

        if rfs[14] == 0:
            add("%s", "?", 3)
        else:
            # ir_xi_res
            add("%.2f", get_packed(rfs[14], 9, 5, 4) / 100 - 50)
            # ir_eta_res
            add("%.2f", get_packed(rfs[14], 9, 1, 4) / 100 - 50)
            # ir_calibration_source
            add("%i", get_packed(rfs[14], 9, 0, 1))

        # blue_1_scan_lookback_index
        if rfs[15] == 0:
            add("%s", "?")
        else:
            add("%i", rfs[15])

        # red_1_scan_lookback_index
        if rfs[16] == 0:
            add("%s", "?")
        else:
            add("%i", rfs[16])

        # blue_2_scan_lookback_index
        if rfs[17] == 0:
            add("%s", "?")
        else:
            add("%i", rfs[17])

        # red_2_scan_lookback_index
        if rfs[18] == 0:
            add("%s", "?")
        else:
            add("%i", rfs[18])

        # ir_scan_lookback_index
        if rfs[19] == 0:
            add("%s", "?")
        else:
            add("%i", rfs[19])

        file_out.handle.write(str.join(",", out_fields) + "\n")
        object_counter = object_counter + 1

    file_in.close()

file_out.handle.close()
units = [ "id number in catalog",
          "deg",
          "deg",
          "mas/year",
          "mas/year",
          "0.1",
          "0 or 1",
          "mas/year",
          "mas/year",
          "0.1 arcsec",
          "0.1 arcsec",
          "number of detections",
          "0 or 1 (format: e)",
          "mas",
          "mas",
          "year",
          "0 or 1 (format: f)",
          "mag",
          "field number in survey",
          "survey number (format: h)",
          "similarity on scale 0 - 11 (format: i)",
          "mag",
          "field number in survey",
          "survey number (format: h)",
          "similarity on scale 0 - 11 (format: i)",
          "mag",
          "field number in survey",
          "survey number (format: h)",
          "similarity on scale 0 - 11 (format: i)",
          "mag",
          "field number in survey",
          "survey number (format: h)",
          "similarity on scale 0 - 11 (format: i)",
          "mag",
          "field number in survey",
          "survey number (format: h)",
          "similarity on scale 0 - 11 (format: i)",
          "arcsec",
          "arcsec",
          "photometric calibration source number (format: j)",
          "arcsec",
          "arcsec",
          "photometric calibration source number (format: j)",
          "arcsec",
          "arcsec",
          "photometric calibration source number (format: j)",
          "arcsec",
          "arcsec",
          "photometric calibration source number (format: j)",
          "arcsec",
          "arcsec",
          "photometric calibration source number (format: j)",
          "lookback index into PMM scan file",
          "lookback index into PMM scan file",
          "lookback index into PMM scan file",
          "lookback index into PMM scan file",
          "lookback index into PMM scan file" ]

assert len(columns) == len(units), "Mismatch between number of columns and units"
desc_file_path = output_folder + "/column-description.txt"
print("Writing descriptions of columns in output to: " + output_folder)
desc_file = open(desc_file_path, mode="w")
desc_file.write("In the units column, \"format: X\" refers to\nfootnotes in the input-data-format.html or\nhttp://tdc-www.harvard.edu/catalogs/ub1.format.html\n\n")
desc_file.write("column name" + ' ' * 19 + "unit" + "\n")
desc_file.write("-" * 34 + "\n")

for i in range(0, len(columns)):
    c = columns[i]
    u = units[i]
    desc_file.write(c + ' ' * (30-len(c)) + u + "\n")

desc_file.close()