from mi.core.persistent_store import PersistentStoreDict

persistentStoreDict = PersistentStoreDict("rasfl", "GI01SUMO-00001")

##################
## START TEST 1 ##
##################
print("## START TEST 1 ##")

# Test 1a - Create Records [Success]
persistentStoreDict["Key 1"] = "blah"
print(persistentStoreDict.keys())
persistentStoreDict["Key 2"] = 1234
print(persistentStoreDict.keys())
persistentStoreDict["Key 3"] = 56.78
print(persistentStoreDict.keys())
persistentStoreDict["Key 4"] = True
print(persistentStoreDict.keys())

# Test 1b - Create Records [Fail - Bad Key Type]
try:
	persistentStoreDict[5555] = "fail"
	print("Test 1b - This should not print.")
except TypeError as e:
	print(e.args[0])

# Test 1c - Create Records [Fail - Bad Item Type]
try:
	tempList = [1, 2, 3, 4, 5]
	persistentStoreDict["Key 5"] = tempList
	print("Test 1c - This should not print.")
except TypeError as e:
	print(e.args[0])

##################
## START TEST 2 ##
##################
print("## START TEST 2 ##")

# Test 2a - Get Records [Success]
key1 = persistentStoreDict["Key 1"]
print(type(key1).__name__ + " : " + str(key1))
key2 = persistentStoreDict["Key 2"]
print(type(key2).__name__ + " : " + str(key2))
key3 = persistentStoreDict["Key 3"]
print(type(key3).__name__ + " : " + str(key3))
key4 = persistentStoreDict["Key 4"]
print(type(key4).__name__ + " : " + str(key4))

# Test 2b - Get Records [Fail - Bad Key Type]
try:
	keyFail = persistentStoreDict[5555]
	print("Test 2b - This should not print.")
except TypeError as e:
	print(e.args[0])

# Test 2c - Get Records [Fail - Key Not Found]
try:
	key5 = persistentStoreDict["Key 5"]
	print("Test 2c - This should not print.")
except KeyError as e:
	print(e.args[0])

##################
## START TEST 3 ##
##################
print("## START TEST 3 ##")

# Test 3 - Update Records [Success]
persistentStoreDict["Key 1"] = "tree"
key1 = persistentStoreDict["Key 1"]
print(type(key1).__name__ + " : " + str(key1))
print(persistentStoreDict.keys())
persistentStoreDict["Key 2"] = 5678
key2 = persistentStoreDict["Key 2"]
print(type(key2).__name__ + " : " + str(key2))
print(persistentStoreDict.keys())
persistentStoreDict["Key 3"] = 12.34
key3 = persistentStoreDict["Key 3"]
print(type(key3).__name__ + " : " + str(key3))
print(persistentStoreDict.keys())
persistentStoreDict["Key 4"] = False
key4 = persistentStoreDict["Key 4"]
print(type(key4).__name__ + " : " + str(key4))
print(persistentStoreDict.keys())

##################
## START TEST 4 ##
##################
print("## START TEST 4 ##")

# Test 4a - Remove Records [Success]
del persistentStoreDict["Key 1"]
print(persistentStoreDict.keys())
del persistentStoreDict["Key 2"]
print(persistentStoreDict.keys())
del persistentStoreDict["Key 3"]
print(persistentStoreDict.keys())
del persistentStoreDict["Key 4"]
print(persistentStoreDict.keys())

# Test 4b - Remove Records [Fail - Bad Key Type]
try:
	del persistentStoreDict[5555]
	print("Test 4b - This should not print.")
except TypeError as e:
	print(e.args[0])

# Test 4c - Remove Records [Fail - Key Not Found]
try:
	del persistentStoreDict["Key 5"]
	print("Test 4c - This should not print.")
except KeyError as e:
	print(e.args[0])

