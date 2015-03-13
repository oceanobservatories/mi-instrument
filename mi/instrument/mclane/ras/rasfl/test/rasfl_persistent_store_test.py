from mi.instrument.mclane.ras.rasfl.rasfl_persistent_store import RasflPersistentStoreDict

rasflPersistentStoreDict = RasflPersistentStoreDict("GI01SUMO-00001")

##################
## START TEST 1 ##
##################
print("## START TEST 1 ##")

# Test 1a - Set Collection Bag Info [Success]
print(rasflPersistentStoreDict.keys())
rasflPersistentStoreDict.setCollectionBagInfo(5)
print(rasflPersistentStoreDict.keys())

# Test 1b - Set Collection Bag Info [Fail - Bad Type]
try:
	rasflPersistentStoreDict.setCollectionBagInfo("fail")
	print("Test 1b - This should not print.")
except TypeError as e:
	print(e.args[0])

##################
## START TEST 2 ##
##################
print("## START TEST 2 ##")

# Test 2 - Use Collection Bags [Success & Fail]
try:
	loopCount = 0
	while True:		
		print("Iteration #{}".format(loopCount))
		print(rasflPersistentStoreDict.keys())
		print(rasflPersistentStoreDict.canUseCollectionBag())
		rasflPersistentStoreDict.useCollectionBag()
		loopCount += 1
except Exception as e:
	print(e.args[0])

##################
## START TEST 3 ##
##################
print("## START TEST 3 ##")

# Test 3 - Delete Collection Bag Info [Success]
print(rasflPersistentStoreDict.keys())
rasflPersistentStoreDict.delCollectionBagInfo()
print(rasflPersistentStoreDict.keys())

