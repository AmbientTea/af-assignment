#!/usr/bin/env bash
set -x
tail -n +2 Views.csv  | sort -t, -k 1 > Views.csv.sorted
tail -n +2 Clicks.csv | sort -t ',' -k 4  > Clicks.csv.sorted
tail -n +2 ViewableViews.csv | sort -t ',' -k 4  > ViewableViews.csv.sorted

join -t ',' -1 1 -2 4 Views.csv.sorted Clicks.csv.sorted | cut -d , -f 2,5 > clicks_views_delay.csv
join -t ',' -1 1 -2 3 Views.csv.sorted ViewableViews.csv.sorted | cut -d , -f 2,5 > views_views_delay.csv

head -n 1 clicks_views_delay.csv
head -n 1 views_views_delay.csv

cat clicks_views_delay.csv views_views_delay.csv | ./delays.py

