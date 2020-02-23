# Description

Joining of streams using a sliding temporal window using akka-streams.

# Running

`sbt run <views file> <clicks file> <viewable events file>`

# Considerations

Analysis of test data provided shows that 95% percent of matching entries
are within 30 minutes of eachother. [`analysis.sh`], so this window was chosen
as the default in the implementation.

Data arriving is assumed to be mostly sorted, with time difference between
the displaced data points being much smaller than the assumed time window.
