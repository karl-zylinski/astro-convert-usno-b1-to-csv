# Written by Karl Zylinski (karl@zylinski.se)

# Counts errors in USNO B1 data

import os
import struct

start_zone_number = 0
end_zone_number = 1799
packed_ints_per_row = 20
row_length = 80
num_bad_objects_field_5_zero = 0
prev_folder = ""

for zone_number in range(start_zone_number, end_zone_number + 1):
    folder_name = ("%04i" % zone_number)[0:3]

    if folder_name != prev_folder:
        print("Entering %s" % folder_name)
        prev_folder = folder_name

    file_in_path = "input-data/%s/b%04i.cat" % (folder_name, zone_number)
    file_in = open(file_in_path, mode = "rb")
    file_in_size = os.path.getsize(file_in_path)
    file_in.seek(12, 1)

    # This processes the current .cat-file, row for row
    while file_in.tell() < file_in_size:
        fifth_field = int.from_bytes(file_in.read(4), byteorder='little', signed=True)

        if fifth_field < 0:
            num_bad_objects_field_5_zero = num_bad_objects_field_5_zero + 1
        
        file_in.seek(72, 1)

    file_in.close()

print("Number of bad objects (negative fifth field): %i" % num_bad_objects_field_5_zero)
