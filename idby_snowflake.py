from snowflake import SnowflakeGenerator
from datetime import datetime
from time import sleep, time

# set start time to Jan 1st 2023 00:00:00
start_time = datetime(2023, 1, 1, 0, 0, 0)
machine_id = 80
sfg = SnowflakeGenerator(instance=machine_id, 
                         epoch=int(start_time.timestamp() * 1000.))

for _ in range(20):
    snowflake_id = next(sfg)
    ss = "{0:64b}".format(snowflake_id)
    print(ss)
    sleep(1.)

# print(len('          11010001001001110000100011000001'))
