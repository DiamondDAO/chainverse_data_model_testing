import glob
import json
import os
import sqlite3


def create_connection(db_file):
    """Create a connection to a local database

    Args:
        db_file (str): name of the database to connect to (or create if not exists)
    """
    connection = None
    try:
        connection = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)

    return connection

def create_table(conn, table, field_dict):
    """Create a new table in database specified in the connection.

    Args:
        conn (sqlite3 connection): connection to the database.
        table (str): name of the table to create
        field_dict (dict): dictionary with {"field_name": "data_type"} structure
    """
    cursor = conn.cursor()

    # drop the table if it already exists because we're simply testing.
    try:
        cursor.execute("DROP TABLE " + table)
    except:
        pass
    
    # parse the `field_dict` to create sql statement
    fields = "".join([str(key) + " " + str(value) + ", " for key, value in field_dict.items()])[:-2]
    
    # create the table
    sql = "CREATE TABLE " + table + " (" + fields + ")"
    cursor.execute(sql)



def insert_row(conn, table, row_data):
    """Insert a single row of DAO data into a table.

    Args:
        conn (sqlite3.connection): sqlite3 connection to the database.
        table (str): database table to insert records into.
        row_data (tuple): tuple of data to insert into columns of the dao table.
    """

    sql = "INSERT INTO " + table + " VALUES(" + "?," * (len(row_data) - 1) + "?)"

    cursor = conn.cursor()
    cursor.execute(sql, row_data)
    conn.commit()
    return cursor.lastrowid

dao_mapping = {
    "id": "DAO_ID",
    "summoningTime": "summonDt",
    "totalShares": "totalShares",
    "totalLoot": "totalLoot"
}

member_mapping = {
    "id": "MEMBER_ID",
    "joinDt": "createdAt",
    "kickedStatus": "kicked",
    "jaledStatus": "jailed",
    "numShares": "shares",
    "numLoot": "loot"
}

tbl_fields_dao = {
    "DAO_ID": "TEXT",
    "network": "TEXT",
    "summonDt": "INTEGER",
    "totalShares": "INTEGER",
    "totalLoot": "INTEGER",
    "snapshotDt": "INTEGER",
    "snapshotBlockID": "INTEGER"
}

tbl_fields_member = {
    "MEMBER_ID": "TEXT",
    "joinDt": "INTEGER",
    "kickedStatus": "INTEGER",
    "jailedStatus": "INTEGER",
    "numShares": "INTEGER",
    "numLoot": "INTEGER",
    "snapshotDt": "INTEGER",
    "snapshotBlockID": "INTEGER"
}

def create_dao_record(json_data, filename):
    """Create a tuple for a single record in the `dao` table.

    Args:
        json_file (json): json file from the daohaus subgraph
        filename (str): actual filename of the json file
    
    Returns:
        row (tuple): tuple with values to insert in a single row.
    """
    # data = json.load(json_file)
    
    # snapshot block id
    blockId = filename.split("_")[1]

    row = dict()

    # create dictionary to represent one row of data in the table
    row["id"] = json_data["id"]
    row["network"] = "xdai"
    row["summoningTime"] = json_data["summoningTime"]
    row["totalShares"] = json_data["totalShares"]
    row["totalLoot"] = json_data["totalLoot"]
    row["snapshotDt"] = 999 # need to include snapshot date as well as blockid somehow
    row["snapshotBlockID"] = blockId
    
    return tuple(row.values())

def create_member_records(json_data, filename):
    """Create a tuple for a single record in the `member` table.

    Args:
        json_file (json): json file from the daohaus subgraph
        filename (str): actual filename of the json file
    
    Returns:
        row (tuple): tuple with values to insert in a single row.
    """
    # data = json.load(json_file)

    # snapshot block id
    blockId = filename.split("_")[1]

    row = dict()

    # create dictionary to represent one row of data in the table
    row["id"] = json_data["id"].split("-")[2]
    row["joinDt"] = json_data["createdAt"]
    row["kickedStatus"] = json_data["kicked"]
    row["jailedStatus"] = json_data["jailed"]
    row["numShares"] = json_data["shares"]
    row["numLoot"] = json_data["loot"]
    row["snapshotDt"] = 999 # need to include snapshot date as well
    row["snapshotBlockID"] = blockId

    return tuple(row.values())    

def main():
    # create database connection
    db = "chainverse.db"
    connection = create_connection(db)

    # using the connection to the database
    with connection:
        # create `dao` table
        create_table(connection, "dao", tbl_fields_dao)

        # create `member` table
        create_table(connection, "member", tbl_fields_member)

        # insert `dao` table with records
        for filename in glob.glob(os.path.join("./data", "*.json")):
            # row = dict()
            with open(filename, encoding="utf-8", mode="r") as currentFile:
                data = json.load(currentFile)

                # Create the record for the dao table and insert into dao table.
                dao_row = create_dao_record(data, filename)
                insert_row(connection, "dao", dao_row)

                # Create the record for the dao table.
                for member in data["members"]:
                    member_row = create_member_records(member, filename)
                    insert_row(connection, "member", member_row)
    connection.close()
                
if __name__ == "__main__":
    main()