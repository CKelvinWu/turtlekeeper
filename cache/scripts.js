module.exports = {
  getNewMasterScript: `
  local data = redis.call("GET", KEYS[1])
  if (data) then
    return false
  end

  local replicas = redis.call("HKEYS", KEYS[2])
  if (#replicas == 0) then
    return false
  end
  
  local rand = math.random(#replicas)
  local newMaster = replicas[rand]
  redis.call("SET", KEYS[1], newMaster)
  redis.call("HDEL", KEYS[2], newMaster)

  return newMaster`,
  voteNewMasterScript: `  
  local hostIp = KEYS[1]
  local myIp = KEYS[2]
  local REPLICA_KEY = KEYS[3]
  local MASTER_KEY = KEYS[4]
  local currentTime = tonumber(ARGV[1])
  local voteCountTime = tonumber(ARGV[2])
  local quorum = tonumber(ARGV[3])
  local isReplica = tonumber(ARGV[4])
  
  redis.call('HSET', hostIp, myIp, currentTime)
  local votes = redis.call('HVALS', hostIp)
  local vote = 0
  for i=1,#votes do
    if (currentTime - votes[i] < voteCountTime) then
      vote = vote + 1
    end
  end

  if (vote ~= quorum) then
    return { 0, 0, 0 }
  else
    redis.call('DEL', hostIp)
    if (isReplica == 1) then
      redis.call('HDEL', REPLICA_KEY, hostIp)
      return { 1, 0, 0 }
    else
      local replicas = redis.call('HKEYS', REPLICA_KEY)

      if (#replicas == 0) then
        redis.call('DEL', MASTER_KEY);
        return { 1, 0, 0 }
      else
        local rand = math.random(#replicas)
        local newMaster = replicas[rand]
        redis.call('HDEL', REPLICA_KEY, newMaster)
        redis.call('SET', MASTER_KEY, newMaster)
        return { 1, 1, newMaster } 
      end
    end
  end  
  `,
};
