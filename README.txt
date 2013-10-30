PyThesis
========

Thesis Abstract
---------------

Through this report, we highlight an existing need for high performance visualisation techniques with the capability of responding to a GeoJSON or Well Known Test (WKT) request for a 1,000,000 point data set within 1 second. We demonstrate that existing geospatial visualisation techniques are unable to process and transmit GeoJSON or WKT requests of this size, in the desired time frame.

We demonstrate that through the use of a common geospatial visualisation technique abstraction, we are able to create a test platform, capable of testing a visualisation technique’s pre-processing, database usage, request processing, and transmission requirements. This common abstraction is abstract enough as to allow visualisation techniques with vastly different requirements to be directly compared.

Five techniques were designed and implemented to allow for the testing of the benefits of using bounding box, gridded clustering and caching strategies to process geospatial visualisation requests. Testing demonstrated that using the strategies in isolation did improve performance, but performance improvements capable of reaching our design goal were only possible through coupling the strategies together.

Results showed that, when not using any strategies, a GeoJSON request for a dataset of up to 22,000 points could be processed within a second. When using a bounding box strategy in isolation, a GeoJSON request with a defined bounding box containing up to 5,200 points could be processed within a second, regardless of the number of points in the entire data set. When using the gridded clustering and bounding strategies together, a request with up to 100,000 points within the defined bounding box could be processed within a second. When using a bounding box to select from a cache table of pre-processed gridded clusters, a GeoJSON request could be processed with up to 1,000,000 points. Significant draw-backs of using a cache table were identified: pre-processing took up to 160 minutes, and the database grew up to 567% in size.

We proposed that for most data sets with up to 1,000,000 data points, using a combination of the bounding box and gridded clustering strategies would result in significantly improved performance, and reduced transmission times. We also proposed that for data sets that change infrequently, and where sufficient database storage was available, a cache table could be introduced to further improve performance.

Method
-------------

The following describe how to run this code using Vagrant
on a local VM.

1. Install Virtualbox

2. Install Vagrant

3. At a terminal, move to this directory

4. Create the VM (run from a terminal)

    vagrant up

5. SSH into the VM (run from a terminal)

    vagrant ssh

6. cd to /vagrant directory

    cd /vagrant

7. Run the test scenario

    ./bin/test_scenario_one development.ini


To change the input data, make sure your input csv files are in the folder:

    thesis/tests/fixtures/

and then update the LAYER_NAMES in the file:

    thesis/scripts/seed_db.py

Once that's done, re-run the test_scenario_one script
