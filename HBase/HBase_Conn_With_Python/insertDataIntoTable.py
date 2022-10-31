import happybase as hb
import time


try:
    print("Trying to make connection with HBase.......")
    conn = hb.Connection('hostname/ip address')
    print("Connection Established")
except:
    print("Connection falied")


try: 
    print("Fetching table .....")
    table = conn.table('CollegeSystem')
    print("table fetched.")
    
except:
    print("some error has been occured! please try again")


try:
    print("Inserting Items ......")
    time.sleep(2)
    table.put(
        b'BCA', {
            b'college_id:number': b'1975',
            b'Name:collegeName': b'UIM',
            b'Name:StdName':b'Vipul Pandey',
            b'address:stdAddress':b'UP Prayagraj',
            b'address:clgAddress': b'Naini Prayagraj'
        }
    )
    print("Items have been inserted.")
except:
    print("some error has been occured! please try again")


try:
    print("Scaning the data from table ....")
    time.sleep(3)
    for key, data in table.scan():
        print(key, data)

except:
    print("some error has been occured! please try again")
