
class SYS_VIEW:

	# Contructor
	def __init__(self, view_string, shard_count):
		# List of all replicas, will be easier for resharding
		self.__replicaList = view_string.split(",")
		# Dictionary of shard views, key is the shard ID
		self.__view = {shardID: [] for shardID in range(shard_count)}
		self.__shard_count = shard_count
		i = 0
		for replica in replicas:
			self.__view[shard_id].append(replica)
        	shard_id += 1
        	if shard_id == self.__shard_count:
            	shard_id = 0
    # Get the shard ID for a particular replica,
    # return -1 if replica does not exist
    def getShardID(self, replica):
    	for shard_id in self.__view:
        	if replica in self.__view[shard_id]:
            	return shard_id
    	return -1
    # Get string version of all the shard id's in the system
    def getShardIds(self):
    	id_str = ""
    	for id in self.__view:
        	id_str += (str(i) + ',')
    	id_str = id_str.rstrip(',')
    	return id_str
    # Return a list of replicas in the same shard as the given replica
    def getShardMembers(self, replica):
    	for shard_id in self.__view:
    		if replica in self.__view[shard_id]:
    			return self.__view[shard_id]
    	return -1
    # Remove a replica from the system view
    def removeReplica(self, replica):
    	self.__replicaList.remove( replica )
    	for shard_id in self.__view:
    		if replica in self.__view[shard_id]:
    			self.__view[shard_id].remove( replica )
    # Add a replica to the system view
    def addReplica( self, replica ):
    	self.__replicaList.append( replica )

    # Get system view
    def getSYS_VIEW(self):
    	return self.__replicaList
    # Get number of shards in the system
    def getShardCount(self)
    	return self.__shard_count