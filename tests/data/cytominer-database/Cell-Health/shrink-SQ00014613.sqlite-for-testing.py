"""
Creates small test dataset from SQ00014613.sqlite for testing purposes.

Source:
https://nih.figshare.com/articles/dataset/
Cell_Health_-_Cell_Painting_Single_Cell_Profiles/9995672?file=18506036
"""

import shutil
import sqlite3

sqlite_source = "SQ00014613.sqlite"
sqlite_target = "test-SQ00014613.sqlite"

# note: we presume the pre-existence of SQ00014613.sqlite
# from an earlier download outside of this python work.
shutil.copy(sqlite_source, sqlite_target)

with sqlite3.connect(sqlite_target) as conn:
    # delete data except that related to two tablenumbers
    conn.execute(
        """
        DELETE FROM Image 
        WHERE TableNumber NOT IN 
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
            OR ObjectNumber > 6
            """
        )

    conn.commit()
    conn.execute("VACUUM;")
    conn.commit()
