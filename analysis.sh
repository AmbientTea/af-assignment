#!/usr/bin/env bash

echo $(tail -n +2 Views.csv | cut -d, -f3  | sort -u  | wc -l)" unique campaign ids"

tail -n +2 Views.csv  | sort -t, -k 1 > Views.csv.sorted
tail -n +2 Clicks.csv | sort -t ',' -k 4  > Clicks.csv.sorted
tail -n +2 ViewableViewEvents.csv | sort -t ',' -k 4  > ViewableViewEvents.csv.sorted

join -t ',' -1 1 -2 4 Views.csv.sorted Clicks.csv.sorted | cut -d , -f 2,5 > clicks_views_delay.csv
join -t ',' -1 1 -2 3 Views.csv.sorted ViewableViewEvents.csv.sorted | cut -d , -f 2,5 > views_views_delay.csv

cat clicks_views_delay.csv views_views_delay.csv | ./delays.py
