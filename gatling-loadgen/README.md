Gatling db load generator using jdbc & base Gatling w/o any extensions.
* Note this is just a hack over http dsl to run a jdbc test, so report generation should be suppressed otherwise the build will results in failure at the end.

To run the tests 
 ./mvnw  -e gatling:test -Dgatling.noReports=true

we have db2 test under db2loadgen and oracle under oraloadgen folder.