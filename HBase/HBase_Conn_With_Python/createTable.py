import happybase as hb 

try:
    print("Trying to make connection with HBase.......")
    conn = hb.Connection('hostname/ipaddress')
    print("Connection Established")
except:
    print("Connection falied")

try:
    print("Trying to make a new table in HBase")
    conn.create_table(
        'org',
        {
            depart_id: dict(),
            depart_name: dict()
        }
    )
    print("Table has been created successfully")
except:
    print("some error has been occured")

