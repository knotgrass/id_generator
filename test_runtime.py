from snowflake import SnowflakeGenerator
from snowflake.snowflake import  MAX_TS, MAX_INSTANCE, MAX_SEQ
from datetime import datetime
from typing import Optional
from time import sleep, time


class New_SnowflakeGenerator(SnowflakeGenerator):
    def __init__(self, instance: int, *, seq: int = 0, epoch: int = 0, timestamp: Optional[int] = None):

        current = int(time() * 1000.)

        if current >= MAX_TS:
            raise OverflowError(f"The maximum timestamp has been reached in selected epoch,"
                                f"so Snowflake cannot generate more IDs!")

        timestamp = timestamp or current

        if timestamp < 0 or timestamp > current:
            raise ValueError(f"timestamp must be greater than 0 and less than {current}!")

        if epoch < 0 or epoch > current:
            raise ValueError(f"epoch must be greater than 0 and lower than current time {current}!")

        self._epo = epoch
        self._ts = timestamp - self._epo

        if instance < 0 or instance > MAX_INSTANCE:
            raise ValueError(f"instance must be greater than 0 and less than {MAX_INSTANCE}!")

        if seq < 0 or seq > MAX_SEQ:
            raise ValueError(f"seq must be greater than 0 and less than {MAX_SEQ}!")

        self._inf = instance << 12
        self._seq = seq
        
    def __next__(self) -> Optional[int]:
        current = int(time() * 1000.) - self._epo

        if self._ts == current:
            if self._seq == MAX_SEQ:
                return None
            self._seq += 1
        elif self._ts > current:
            return None
        else:
            self._seq = 0

        self._ts = current

        return self._ts << 22 | self._inf | self._seq
    

# set start time to Jan 1st 2023 00:00:00
start_time = datetime(2023, 1, 1, 0, 0, 0)
machine_id = 80
epoch=int(start_time.timestamp() * 1000.)

cfg = dict(
    
)
def test_init(n_test:int=1000000):
    # run first time
    sfg     = SnowflakeGenerator(instance=machine_id, epoch=epoch)
    new_sfg = New_SnowflakeGenerator(instance=machine_id, epoch=epoch)

    old = 0.
    new = 0.
    t0 = time()
    t1 = time()
    t2 = time()
    
    # run test
    for _ in range(n_test):
        t0 = time()
        sfg     = SnowflakeGenerator(instance=machine_id, epoch=epoch)
        t1 = time()
        new_sfg = New_SnowflakeGenerator(instance=machine_id, epoch=epoch)
        t2 = time()
        
        old += (t1 - t0)
        new += (t2-t1)

    print(f'runtime old {n_test} time = {old}')
    print(f'runtime new {n_test} time = {new}')
    print()


def test__next__(n_test:int=1000000):
    sfg     = SnowflakeGenerator(instance=machine_id, epoch=epoch)
    new_sfg = New_SnowflakeGenerator(instance=machine_id, epoch=epoch)
    
    # run 1st time
    old_id = next(sfg)
    new_id = next(new_sfg)
    old = 0.
    new = 0.
    t0 = time()
    t1 = time()
    t2 = time()
    
    # run test
    for _ in range(n_test):
        t0 = time()
        old_id = next(sfg)
        t1 = time()
        new_id = next(new_sfg)
        t2 = time()
        
        old += (t1 - t0)
        new += (t2-t1)

    print(f'runtime old {n_test} time = {old}')
    print(f'runtime new {n_test} time = {new}')
    print()
    

if __name__ == '__main__':
    sleep(300)
    print('start benchmark')
    N_test = 1000000000
    test_init(N_test)
    test__next__(N_test)
