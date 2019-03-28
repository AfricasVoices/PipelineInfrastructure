import argparse
import datetime
from firestore_uuid_table import FirestoreUuidTable

parser = argparse.ArgumentParser(description="Run test operations on a id mapping table")
parser.add_argument("crypto_token_path", help="Path to .json file containing authentication details to firestore", nargs=1)

args = parser.parse_args()
CRYPTO_TOKEN = args.crypto_token_path[0]


TEST_TABLE_NAME = "phone-no-test"
PREFIX="phone-no-test-"

fsut = FirestoreUuidTable(
    TEST_TABLE_NAME, 
    CRYPTO_TOKEN,
    PREFIX)

print ("Testing Addition + lookup success")
for i in range(0, 10):
    data_original = "Test lookup-{}".format(i)
    uuid = fsut.data_to_uuid(data_original)
    print (f"Data:          {data_original}")
    print (f"UUID:          {uuid}")

    data_readback = fsut.uuid_to_data(uuid)
    print (f"Data Readback: {data_readback}")

    assert data_original == data_readback, "Readback failure"


print ("Testing lookup failure")
try:
    fsut.uuid_to_data("_______ Not present UUID")
    assert False, "Lookup didn't throw as expected"
except:
    print (f"Lookup failed as expected")



# Execute batch get data -> uuid
print ("Testing Batch + lookup success")
now_1 = datetime.datetime.now().isoformat()
mapping_1 = fsut.data_to_uuid_batch([
    "Test lookup-1",    # this will be in datastore
    "Test lookup-2",    # this will be in datastore
    "Test lookup-" + now_1, # this won't be
    "Test lookup-2-" + now_1, # this won't be
])
print (mapping_1)

# spin lock
while datetime.datetime.now().isoformat() == now_1:
    pass

now_2 = datetime.datetime.now().isoformat()
mapping_2 = fsut.data_to_uuid_batch([
    "Test lookup-1",    # this will be in datastore
    "Test lookup-2",    # this will be in datastore
    "Test lookup-" + now_2, # this won't be
    "Test lookup-2-" + now_2, # this won't be
])
print (mapping_2)

assert mapping_1["Test lookup-1"] == mapping_2["Test lookup-1"]
assert mapping_1["Test lookup-2"] == mapping_2["Test lookup-2"]
assert mapping_1["Test lookup-" + now_1] != mapping_2["Test lookup-" + now_2]
assert mapping_1["Test lookup-2-" + now_1] != mapping_2["Test lookup-2-" + now_2]


# Execute batch get uuid -> data
uuid_1 = fsut.data_to_uuid("Test lookup-1")
uuid_2 = fsut.data_to_uuid("Test lookup-2")

print ("Testing Batch + lookup success")
lookup_1 = fsut.uuid_to_data_batch([
    uuid_1,    # this will be in datastore
    uuid_2,    # this will be in datastore
    "This won't be a uuid", # this won't be
])
print (lookup_1)

assert len(lookup_1) == 2
assert lookup_1[uuid_1] == "Test lookup-1"
assert lookup_1[uuid_2] == "Test lookup-2"
