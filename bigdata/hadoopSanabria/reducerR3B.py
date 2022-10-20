#!/usr/bin/python

"""reducer.py"""


from operator import itemgetter
import sys

current_id = None
sum_score = 0
parent_id = None
n = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    parent_id, score = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        score = int(score)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_id == parent_id:
        sum_score += score
        n += 1
    else:
        if current_id:
            # write result to STDOUT
            mean = round((sum_score / n), 2)
            print ('%s\t%s' % (current_id, mean))
        sum_score = score
        current_id = parent_id
        n = 1

# do not forget to output the last word if needed!
if current_id == parent_id:
    mean = round((sum_score / n), 2)
    print ('%s\t%s' % (current_id, mean))