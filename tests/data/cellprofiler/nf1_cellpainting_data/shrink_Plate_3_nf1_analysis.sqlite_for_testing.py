"""
Creates small test dataset from Plate_3_nf1_analysis.sqlite for testing purposes.

Source:
https://github.com/WayScience/nf1_cellpainting_data/raw/main/2.cellprofiler_analysis/analysis_output/Plate_3/Plate_3_nf1_analysis.sqlite
"""

# disable similar line checks for pylint
# pylint: disable=R0801

import shutil
import sqlite3

SQLITE_SOURCE = "Plate_3_nf1_analysis.sqlite"
SQLITE_TARGET = "test-Plate_3_nf1_analysis.sqlite"

# note: we presume the pre-existence of Plate_3_nf1_analysis.sqlite
# from an earlier download outside of this python work.
shutil.copy(SQLITE_SOURCE, SQLITE_TARGET)

with sqlite3.connect(SQLITE_TARGET) as conn:
    # delete data except that related to two tablenumbers
    conn.execute(
        """
        DELETE FROM Per_Image
        /* use site and well which are known to
        contain imagenumbers that don't persist
        to compartment tables */
        WHERE Image_Metadata_Site != '1'
        AND Image_Metadata_Well != 'B1';
        """
    )
    # do the same for compartment tables, also removing objectnumbers > 3
    for table in ["Cells", "Nuclei", "Cytoplasm"]:
        conn.execute(
            f"""
            DELETE FROM Per_{table}
            WHERE
            /* filter using only imagenumbers which exist in modified
            image table after deletions */
            ImageNumber NOT IN (SELECT ImageNumber FROM Per_Image)
            /* Here we limit the number of objects which are returned
            for each compartment table so as to keep the test dataset
            very small. */
            OR {table}_Number_Object_Number > 2
            """
        )

    conn.commit()
    conn.execute("VACUUM;")
    conn.commit()
