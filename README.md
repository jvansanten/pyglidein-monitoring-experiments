# pyglidein monitoring experiments

Extract records with e.g.

python query.py jobs --site DESY --after 2020-03-14 | gzip > condor_jobs.DESY.2020-03-14.2020-03-24.json.gz
python query.py slots --site DESY --after 2020-03-14 | gzip > condor_slots.DESY.2020-03-14.2020-03-24.json.gz

(requires Python >= 3.6, elasticssearch, and elasticsearch-dsl. Must be run from a location where you can reach elk-1.icecube.wisc.edu)

Combine these into a time series of resource proffers and claims with the accompanying notebook.
