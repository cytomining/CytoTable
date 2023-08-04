"""
Creates small test dataset from SQ00014613.sqlite for testing purposes.

Source:
https://nih.figshare.com/articles/dataset/
Cell_Health_-_Cell_Painting_Single_Cell_Profiles/9995672?file=18506036
"""

import shutil
import sqlite3

SQLITE_SOURCE = "SQ00014613.sqlite"
SQLITE_TARGET = "test-SQ00014613.sqlite"

# note: we presume the pre-existence of SQ00014613.sqlite
# from an earlier download outside of this python work.
shutil.copy(SQLITE_SOURCE, SQLITE_TARGET)

with sqlite3.connect(SQLITE_TARGET) as conn:
    # delete data except that related to two tablenumbers
    conn.execute(
        """
        DELETE FROM Image
        WHERE TableNumber NOT IN
        /* TableNumber 88ac13033d9baf49fda78c3458bef89e includes
        mixed-type data which is important to test as part of
        this work. For example, as found in Nuclei column
        Nuclei_Correlation_Costes_AGP_DNA */
        ('88ac13033d9baf49fda78c3458bef89e',
        '1e5d8facac7508cfd4086f3e3e950182')
        """
    )
    # do the same for compartment tables, also removing objectnumbers > 3
    for table in ["Cells", "Nuclei", "Cytoplasm"]:
        conn.execute(
            f"""
            DELETE FROM {table}
            WHERE TableNumber NOT IN (SELECT TableNumber FROM Image)
            /* Here we limit the number of objects which are returned
            for each compartment table so as to keep the test dataset
            very small. */
            OR ObjectNumber > 6
            """
        )

    conn.commit()
    conn.execute("VACUUM;")
    conn.commit()
