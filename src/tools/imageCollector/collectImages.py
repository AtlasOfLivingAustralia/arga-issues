fields = [
    "type",
    "format",
    "identifier",
    "references",
    "title",
    "description",
    "created",
    "creator",
    "contributor",
    "publisher",
    "audience",
    "source",
    "license",
    "rightsHolder",
    "datasetID",
    "taxonName",
    "width",
    "height"
]

# Hacky importing but it works

from locations.vicMuseum import vicMuseum
from locations.flickr import flickr
from locations.inaturalist import inaturalist, countSpecies

print("Collecting Vic Museum...")
vicMuseum.run()

print("Collecting flickr...")
# In the flickr folder make sure you have a flickrkey.txt and flickrusers.txt
# users should have the userid of eadch user to collect from on each line
# key should have the api key as the first line and the secret as the second
flickr.run()

print("Collefcting inaturalist...")
# Make sure you download inaturalist dump from https://inaturalist-open-data.s3.amazonaws.com/metadata/inaturalist-open-data-latest.tar.gz
# Once extracted, update the "dataFolder" variable string to the new folder name
countSpecies.run()
inaturalist.run()
