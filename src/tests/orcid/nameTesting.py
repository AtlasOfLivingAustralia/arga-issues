import requests
import json

def buildNameQuery(firstName: str, lastName: str, email: str) -> str:
    baseURL = "https://pub.orcid.org/v3.0/search/?q="
    search = f"family-name:{lastName}+AND+given-names:{firstName}"

    if email:
        search += f"+AND+email:{email}"
        
    return baseURL + search

if __name__ == "__main__":
    people = "people.txt"
    with open(people) as fp:
        peopleList = fp.read().strip("\n").split("\n")

    headers = {
        "Content-type": "application/vnd.orcid+json"
    }

    for person in peopleList:
        info = person.split()
        if len(info) == 2:
            firstName, lastName = info
            email = ""
        elif len(info) == 3:
            firstName, lastName, email = info
        else:
            print(f"Bad entry: {person}")
            continue

        query = buildNameQuery(firstName, lastName, email)
        response = requests.get(query, headers=headers)
        data = response.json()

        savedFile = f"{firstName}_{lastName}.json"
        with open(savedFile, "w") as fp:
            json.dump(data, fp, indent=4)

        usersFound = data["num-found"]
        print(f"Found user '{firstName} {lastName}' amount: {usersFound}")
        print("*" * 25)

        if usersFound > 0:
            for result in data["result"]:
                print(" "*4 + result["orcid-identifier"]["path"])
        
        print()
