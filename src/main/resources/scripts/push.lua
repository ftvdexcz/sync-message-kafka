local list1 = cjson.decode(ARGV[1])
local list2 = cjson.decode(ARGV[2])

--local fruit = {'orange','apple','watermelon'}
--local deleted = redis.call('SREM', KEYS[1], unpack(list1))
--if deleted > 0 then
--    redis.call('RPUSH', KEYS[2], unpack(list2))
--end
--local a, b = unpack(list1)
--local e1, e2 = unpack(b)


redis.call("SET", "---debug:list1", type(list1))
