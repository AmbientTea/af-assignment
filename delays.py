#!/usr/bin/env python

import sys
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

delays = []
for line in sys.stdin:
    [start, finish] = line.strip().split(',')
    s = datetime.strptime(start, '%Y-%m-%d  %H:%M:%S.%f')
    f = datetime.strptime(finish, '%Y-%m-%d  %H:%M:%S.%f')
    delay = abs((f - s).total_seconds())
    delays.append(delay)

delays.sort()

print "mean delay:", np.mean(delays)
print "95 percentile:", np.percentile(delays, 95)
print "98 percentile:", np.percentile(delays, 98)
print "99 percentile:", np.percentile(delays, 99)


# plt.hist(delays, bins=100)
# plt.show()