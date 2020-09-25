This repository contains code to transform .csv files exported from Mimsy into .csv files suitable for import into CollectionSpace via the cspace-converter.

This is a work in progress, and is based on one client's Mimsy data. I have no idea how much of this would be reusable on another Mimsy->CSpace migration.


To run:

```
bundle install
bundle exec ruby reporting.rb
bundle exec ruby cspace_processing.rb
```

`reporting.rb` will output both problem and informational reports. Uncomment the ones you want to run.

`cspace_processing.rb` produces CSVs for import into CSpace. Uncomment the ones you want to run. The order does not matter because each one calls all its dependencies separately if they haven't already been run by the process. 


