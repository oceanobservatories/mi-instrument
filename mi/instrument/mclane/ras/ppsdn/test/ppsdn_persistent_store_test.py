from mi.instrument.mclane.ras.ppsdn.ppsdn_persistent_store import PpsdnPersistentStoreDict

ppsdnPersistentStoreDict = PpsdnPersistentStoreDict("GI01SUMO-00001")

##################
## START TEST 1 ##
##################
print("## START TEST 1 ##")

# Test 1a - Set Filter Info [Success]
print(ppsdnPersistentStoreDict.keys())
ppsdnPersistentStoreDict.setFilterInfo(5)
print(ppsdnPersistentStoreDict.keys())

# Test 1b - Set Filter Info [Fail - Bad Type]
try:
	ppsdnPersistentStoreDict.setFilterInfo("fail")
	print("Test 1b - This should not print.")
except TypeError as e:
	print(e.args[0])

##################
## START TEST 2 ##
##################
print("## START TEST 2 ##")

# Test 2 - Use Filters [Success & Fail]
try:
	loopCount = 0
	while True:		
		print("Iteration #{}".format(loopCount))
		print(ppsdnPersistentStoreDict.keys())
		print(ppsdnPersistentStoreDict.canUseFilter())
		ppsdnPersistentStoreDict.useFilter()
		loopCount += 1
except Exception as e:
	print(e.args[0])

##################
## START TEST 3 ##
##################
print("## START TEST 3 ##")

# Test 3 - Delete Filter Info [Success]
print(ppsdnPersistentStoreDict.keys())
ppsdnPersistentStoreDict.delFilterInfo()
print(ppsdnPersistentStoreDict.keys())

