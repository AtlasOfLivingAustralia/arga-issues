class Author:
    def __init__(self, data):
        self.firstName = data["firstName"]
        self.lastName = data["lastName"]
        self.affiliation = data.get("affiliation", "")
        self.ror = data.get("affiliationROR", "")

class Funder:
    def __init__(self, data):
        self.organisation = data.get("organization", "")
        self.identifier = data["identifier"]
        self.awardNumber = data.get("awardNumber", "")
        self.identifierType = data.get("identifierType", "")

class DataEntry:
    def __init__(self, data):
        self.links = data["_links"]
        self.href = self.links["self"]["href"]
        self.url = "https://datadryad.org/stash/dataset/" + self.href.split('/')[-1]
        self.identifier = data["identifier"]
        self.id = data["id"]
        self.storageSize = data["storageSize"]
        self.issn = data.get("relatedPublicationISSN", "")
        self.title = data["title"]
        self.abstract = data["abstract"]
        self.keywords = data.get("keywords", [])
        self.locations = [location["place"] for location in data.get("locations", [])]
        self.version = data["versionNumber"]
        self.versionStatus = data["versionStatus"]
        self.curationStatus = data["curationStatus"]
        self.versionChanges = data["versionChanges"]
        self.publicationDate = data["publicationDate"]
        self.lastModificationDate = data["lastModificationDate"]
        self.visibility = data["visibility"]
        self.sharingLink = data["sharingLink"]
        self.userID = data["userId"]
        self.license = data["license"]

        self.authors = [Author(author) for author in data["authors"]]
        self.funders = [Funder(funder) for funder in data.get("funders", [])]

    def __repr__(self):
        return str(self)

    def __str__(self):
        return str(self.data)

    def summarise(self):
        keywordList = '\t\t- ' + '\n\t\t- '.join(word for word in self.keywords)
        return f"{self.title}\n\t- {self.sharingLink}\n\t- Published {self.publicationDate}\n\t- Keywords:\n{keywordList}"

class DryadAPI:
    pass