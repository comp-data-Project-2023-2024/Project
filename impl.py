import sqlite3
import json
import os
from os import sep
import re
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from SPARQLWrapper import SPARQLWrapper, JSON
from pandas import read_csv
import pandas as pd
import requests
from typing import List, Optional, Union, Set
from datetime import datetime
import unittest


# Data Model Classes

class IdentifiableEntity(object): 
    def __init__(self, id: str):
        self.id = id

    def getId(self) -> str:
        return self.id

class Person(IdentifiableEntity): 
    def __init__(self, id: str, name: str):
        super().__init__(id)
        self.name = name

    def getName(self) -> str:
        return self.name

class CulturalHeritageObject(IdentifiableEntity): 
    def __init__(
        self,
        id: str,
        title: str,
        date: Optional[str],
        owner: Optional[str] = None,
        place: Optional[str] = None,
        name: Optional[str] = None, 
        hasAuthor: Union['Person', List['Person'], None] = None,
        author_id: Optional[str] = None,
        author_name: Optional[str] = None,
    ):
        super().__init__(id)
        self.title = title
        self.date = date
        self.owner = str(owner)
        self.place = place
        self.author_id = author_id
        self.author_name = author_name
                
        if isinstance(hasAuthor, Person):
            self.hasAuthor = [hasAuthor]
        elif isinstance(hasAuthor, list):
            self.hasAuthor = hasAuthor
        else:
            self.hasAuthor = []

    def getTitle(self) -> str:
        return self.title

    def getOwner(self) -> str:
        return self.owner

    def getPlace(self) -> str:
        return self.place

    def getDate(self) -> Optional[str]:
        return self.date if self.date else None

    def getAuthors(self) -> List[Person]:
        return self.hasAuthor

class NauticalChart(CulturalHeritageObject):
    pass
class ManuscriptPlate(CulturalHeritageObject):
    pass
class ManuscriptVolume(CulturalHeritageObject):
    pass
class PrintedVolume(CulturalHeritageObject):
    pass
class PrintedMaterial(CulturalHeritageObject):
    pass
class Herbarium(CulturalHeritageObject):
    pass
class Specimen(CulturalHeritageObject):
    pass
class Painting(CulturalHeritageObject):
    pass
class Model(CulturalHeritageObject):
    pass
class Map(CulturalHeritageObject):
    pass

class Activity(object): 
    def __init__(
        self,
        refersTo: CulturalHeritageObject,
        institute: str,
        person: Optional[str],
        tool: Union[str, List[str], None],
        start: Optional[str],
        end: Union[str, List[str], None],
    ):
        self.refersTo = refersTo
        self.institute = institute
        self.person = person
        self.tool = []
        self.start = start
        self.end = end

        if type(tool) == str:
            self.tool.append(tool)
        elif type(tool) == list:
            self.tool = tool

    def getResponsibleInstitute(self) -> str:
        return self.institute

    def getResponsiblePerson(self) -> Optional[str]:
        if self.person:
            return self.person
        return None

    def getTools(self) -> set:
        return self.tool

    def getStartDate(self) -> Optional[str]:
        if self.start:
            return self.start
        return None

    def getEndDate(self) -> Optional[str]:
        if self.end:
            return self.end
        return None

    def refersTo(self) -> CulturalHeritageObject:
        return self.refersTo

class Acquisition(Activity):
    def __init__(
        self,
        refersTo: CulturalHeritageObject,
        institute: str,
        technique: str,
        person: Optional[str],
        start: Optional[str],
        end: Optional[str],
        tool: Union[str, List[str], None],
    ):
        super().__init__(refersTo, institute, person, tool, start, end)
        self.technique = technique

    def getTechnique(self) -> str:
        return self.technique

class Processing(Activity):
    pass
class Modelling(Activity):
    pass
class Optimising(Activity):
    pass
class Exporting(Activity):
    pass

# Additional classes

class Handler(object):

    BLAZEGRAPH_URL = "http://127.0.0.1:9999/blazegraph/sparql"
    SQLITE_FILE_PATH = "data/relational.db" 

    def __init__(self):
        self.db_path = None

    def setDbPathOrUrl(self, path: str) -> bool:
        self.db_path = path
        return True 
    
    def getDbPathOrUrl(self) -> str:
        return self.db_path

class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, file_path: str) -> bool:
        raise NotImplementedError("Subclasses must implement the pushDataToDb method.")

class ProcessDataUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()
        self.setDbPathOrUrl(Handler.SQLITE_FILE_PATH)

    def pushDataToDb(self, file_path: str) -> bool:
        absolute_file_path = file_path 
            
        with open(absolute_file_path, 'r') as f:
                data = json.load(f)
        
        unique_data = {}
        for item in data:
            unique_data[item["object id"]] = item

        clean_data = list(unique_data.values())

        db_file = self.getDbPathOrUrl()
        try:
            conn = sqlite3.connect(db_file)
            c = conn.cursor()

            c.execute("DROP TABLE IF EXISTS Acquisition;")
            c.execute("""CREATE TABLE IF NOT EXISTS Acquisition (
                         object_id TEXT,
                         responsible_institute TEXT,
                         responsible_person TEXT,
                         technique TEXT,
                         tool TEXT,
                         start_date TEXT,
                         end_date TEXT
                       )""")
            c.execute("DROP TABLE IF EXISTS Processing;")
            c.execute("""CREATE TABLE IF NOT EXISTS Processing (
                         object_id TEXT,
                         responsible_institute TEXT,
                         responsible_person TEXT,
                         tool TEXT,
                         start_date TEXT,
                         end_date TEXT
                       )""")
            c.execute("DROP TABLE IF EXISTS Modelling;")
            c.execute("""CREATE TABLE IF NOT EXISTS Modelling (
                         object_id TEXT,
                         responsible_institute TEXT,
                         responsible_person TEXT,
                         tool TEXT,
                         start_date TEXT,
                         end_date TEXT
                       )""")
            c.execute("DROP TABLE IF EXISTS Optimising;")
            c.execute("""CREATE TABLE IF NOT EXISTS Optimising (
                         object_id TEXT,
                         responsible_institute TEXT,
                         responsible_person TEXT,
                         tool TEXT,
                         start_date TEXT,
                         end_date TEXT
                       )""")
            c.execute("DROP TABLE IF EXISTS Exporting;")
            c.execute("""CREATE TABLE IF NOT EXISTS Exporting (
                         object_id TEXT,
                         responsible_institute TEXT,
                         responsible_person TEXT,
                         tool TEXT,
                         start_date TEXT,
                         end_date TEXT
                       )""")
            
            for item in clean_data:
                object_id = item["object id"]
                
                acquisition = item["acquisition"]
                c.execute("""INSERT INTO Acquisition (object_id, responsible_institute, responsible_person, technique, tool, start_date, end_date)
                              VALUES (?, ?, ?, ?, ?, ?, ?)""",
                          (object_id, acquisition["responsible institute"], acquisition["responsible person"],
                           acquisition["technique"], ", ".join(acquisition["tool"]) if acquisition["tool"] else None,
                           acquisition["start date"], acquisition["end date"]))

                processing = item["processing"]
                c.execute("""INSERT INTO Processing (object_id, responsible_institute, responsible_person, tool, start_date, end_date)
                              VALUES (?, ?, ?, ?, ?, ?)""",
                          (object_id, processing["responsible institute"], processing["responsible person"],
                           ", ".join(processing["tool"]) if processing["tool"] else None, processing["start date"],
                           processing["end date"]))
                
                modelling = item["modelling"]
                c.execute("""INSERT INTO Modelling (object_id, responsible_institute, responsible_person, tool, start_date, end_date)
                              VALUES (?, ?, ?, ?, ?, ?)""",
                          (object_id, modelling["responsible institute"], modelling["responsible person"],
                           ", ".join(modelling["tool"]) if modelling["tool"] else None, modelling["start date"],
                           modelling["end date"]))
                
                optimising = item["optimising"]
                c.execute("""INSERT INTO Optimising (object_id, responsible_institute, responsible_person, tool, start_date, end_date)
                              VALUES (?, ?, ?, ?, ?, ?)""",
                          (object_id, optimising["responsible institute"], optimising["responsible person"],
                           ", ".join(optimising["tool"]) if optimising["tool"] else None, optimising["start date"],
                           optimising["end date"]))
                
                exporting = item["exporting"]
                c.execute("""INSERT INTO Exporting (object_id, responsible_institute, responsible_person, tool, start_date, end_date)
                              VALUES (?, ?, ?, ?, ?, ?)""",
                          (object_id, exporting["responsible institute"], exporting["responsible person"],
                           ", ".join(exporting["tool"]) if exporting["tool"] else None, exporting["start date"],
                           exporting["end date"]))
            
            conn.commit()
            print("Data insertion completed successfully.")
            return True
        except sqlite3.Error as e:
            print(f"\nSQLite error: {e}")
            return False
        finally:
            if 'conn' in locals() and conn:
                conn.close()

class MetadataUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()
        self.setDbPathOrUrl(Handler.BLAZEGRAPH_URL)
        self.NauticalChart = URIRef("https://schema.org/NauticalChart")
        self.ManuscriptPlate = URIRef("https://schema.org/ManuscriptPlate")
        self.ManuscriptVolume = URIRef("https://schema.org/ManuscriptVolume")
        self.PrintedVolume = URIRef("https://schema.org/PrintedVolume")
        self.PrintedMaterial = URIRef("https://schema.org/PrintedMaterial")
        self.Herbarium = URIRef("https://schema.org/Herbarium")
        self.Specimen = URIRef("https://schema.org/Specimen")
        self.Painting = URIRef("https://schema.org/Painting")
        self.Model = URIRef("https://schema.org/Model")
        self.Map = URIRef("https://schema.org/Map")
        self.Author = URIRef("https://schema.org/Author")
        self.title = URIRef("https://schema.org/name")
        self.date = URIRef("https://schema.org/dateCreated")
        self.owner = URIRef("https://schema.org/provider")
        self.place = URIRef("https://schema.org/contentLocation")
        self.identifier = URIRef("https://schema.org/identifier")
        self.label = URIRef("http://www.w3.org/2000/01/rdf-schema#label")
        self.hasAuthor = URIRef("https://schema.org/creator")
        self.base_url = "https://github.com/baraldiruffer/ds24project/"

    def pushDataToDb(self, file_path: str) -> bool:
        try:
            heritage = read_csv(
                file_path,
                keep_default_na=False,
                dtype={
                    "Id": "string", 
                    "Type": "string", 
                    "Title": "string",
                    "Date": "string", 
                    "Author": "string", 
                    "Owner": "string",
                    "Place": "string",
                },
            )
        except FileNotFoundError:
            print("Error: CSV file not found.")
            return False
        
        blazegraph_endpoint = self.getDbPathOrUrl()
        my_graph = Graph()

        for idx, row in heritage.iterrows():
            class_uri = None
            if row["Type"] == "Nautical chart": class_uri = self.NauticalChart
            elif row["Type"] == "Manuscript plate": class_uri = self.ManuscriptPlate
            elif row["Type"] == "Manuscript volume": class_uri = self.ManuscriptVolume
            elif row["Type"] == "Printed volume": class_uri = self.PrintedVolume
            elif row["Type"] == "Printed material": class_uri = self.PrintedMaterial
            elif row["Type"] == "Herbarium": class_uri = self.Herbarium
            elif row["Type"] == "Specimen": class_uri = self.Specimen
            elif row["Type"] == "Painting": class_uri = self.Painting
            elif row["Type"] == "Model": class_uri = self.Model
            elif row["Type"] == "Map": class_uri = self.Map

            resource_uri = URIRef(f"{self.base_url}{row['Id']}")
            
            if not row["Date"]:
                row["Date"] = "Unknown"
            
            my_graph.add((resource_uri, RDF.type, class_uri))
            my_graph.add((resource_uri, self.identifier, Literal(row["Id"])))
            my_graph.add((resource_uri, self.title, Literal(row["Title"])))
            my_graph.add((resource_uri, self.date, Literal(row["Date"])))
            my_graph.add((resource_uri, self.owner, Literal(row["Owner"])))
            my_graph.add((resource_uri, self.place, Literal(row["Place"])))

            if row["Author"]:
                text_before_parentheses = row["Author"].split(" (")[0]
                authorID_list = re.findall(r"\((.*?)\)", row["Author"])
                authorID = authorID_list[0] if authorID_list else "noID"
                    
                authorIRI = self.base_url + text_before_parentheses.replace(" ", "_").replace(",", "")

                my_graph.add((resource_uri, self.hasAuthor, URIRef(authorIRI)))
                my_graph.add((URIRef(authorIRI), self.identifier, Literal(authorID)))
                my_graph.add((URIRef(authorIRI), RDF.type, self.Author))
                my_graph.add((URIRef(authorIRI), self.label, Literal(text_before_parentheses)))
            else:
                row["Author"] = "Unknown"
        
        my_graph.serialize(destination="output_triples.ttl", format="ttl")
        
        store = SPARQLUpdateStore()
        store.open((blazegraph_endpoint, blazegraph_endpoint))
        for triple in my_graph.triples((None, None, None)):
            store.add(triple)
        store.close()
        print("Data uploaded to Blazegraph successfully.")
        return True
        

class QueryHandler(Handler):
    def __init__(self):
        super().__init__()

    def getById(self, input_id: str) -> pd.DataFrame:

        if not self.db_path or not self.db_path.startswith(("http://", "https://")):
            return pd.DataFrame()

        is_person_id = ":" in input_id

        person_query = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>

            SELECT DISTINCT ?authorEntity ?authorName ?authorId 
            WHERE {{
                {{ ?authorEntity schema:identifier "{input_id}" . }}
                UNION
                {{ ?authorEntity owl:sameAs ?externalId . FILTER (CONTAINS(STR(?externalId), "{input_id}")) }}
                
                OPTIONAL {{ ?authorEntity rdfs:label ?authorName . }}
                OPTIONAL {{ ?authorEntity schema:identifier ?authorId . }}

            }}
        """

        object_query = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>

            SELECT DISTINCT ?CulturalObject ?label ?type_label ?date ?owner ?place ?authorName ?authorId
            WHERE {{
                
                # Find the Cultural Object using the input ID
                {{ ?CulturalObject schema:identifier "{input_id}" . }}
                UNION
                {{ ?CulturalObject owl:sameAs ?externalId . FILTER (CONTAINS(STR(?externalId), "{input_id}")) }}
                
                OPTIONAL {{ ?CulturalObject rdf:type ?type . }}
                OPTIONAL {{ ?CulturalObject schema:name ?label . }}
                OPTIONAL {{ ?CulturalObject rdfs:label ?label . }}
                OPTIONAL {{ ?CulturalObject schema:dateCreated ?date . }}
                OPTIONAL {{ ?CulturalObject schema:provider ?owner . }}
                OPTIONAL {{ ?CulturalObject schema:contentLocation ?place . }}
                OPTIONAL {{
                    ?CulturalObject schema:creator ?authorEntity .
                    ?authorEntity rdfs:label ?authorName .
                    ?authorEntity schema:identifier ?authorId .
                }}

                BIND(REPLACE(STR(COALESCE(?type, "")), "https://schema.org/", "") AS ?type_label)
            }}
        """
        
        sparql_query = person_query if is_person_id else object_query
        response = requests.get(self.db_path, params={
            'query': sparql_query,
            'format': 'json'
        })

        if response.status_code != 200:
            print(f"SPARQL query failed: {response.status_code}")
            return pd.DataFrame()

        data = response.json()

        results = []
        for binding in data.get("results", {}).get("bindings", []):
            row = {var: binding[var]["value"] for var in binding}
            results.append(row)

        return pd.DataFrame(results)
            
class MetadataQueryHandler(QueryHandler):
    def __init__(self, blazegraph_url: str = Handler.BLAZEGRAPH_URL):
        super().__init__()
        self.setDbPathOrUrl(blazegraph_url)

    def _execute_query(self, query: str) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.db_path)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        
        bindings = results.get("results", {}).get("bindings", [])
        if not bindings:
            return pd.DataFrame()

        df = pd.DataFrame([
            {key: value['value'] for key, value in row.items()}
            for row in bindings
        ])
        return df

    def getAllPeople(self) -> pd.DataFrame:
        sparql_query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?authorName ?authorId
        WHERE {
            ?authorEntity rdf:type schema:Author ;
                    rdfs:label ?authorName ;
                    schema:identifier ?authorId .
        }
        """
        return self._execute_query(sparql_query)

    def getAllCulturalHeritageObjects(self) -> pd.DataFrame:
        sparql_query = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            
            SELECT DISTINCT ?id ?title ?date ?owner ?place ?author_id ?author_name ?type_name 
            WHERE {
                ?entity rdf:type ?type .
                BIND(REPLACE(STR(?type), "https://schema.org/", "") AS ?type_name)
                FILTER(?type_name != "Author") 
                
                OPTIONAL { ?entity schema:name ?title . }
                OPTIONAL { ?entity schema:identifier ?id . }
                
                OPTIONAL { ?entity schema:dateCreated ?date . }
                OPTIONAL { ?entity schema:provider ?owner . } 
                OPTIONAL { ?entity schema:contentLocation ?place . }
                
                OPTIONAL {
                    ?entity schema:creator ?authorEntity .
                    OPTIONAL { ?authorEntity schema:identifier ?author_id . }
                    OPTIONAL { ?authorEntity rdfs:label ?author_name . } 
                }
            }
        """
        return self._execute_query(sparql_query)

    def getAuthorsOfCulturalHeritageObject(self, input_id: str) -> pd.DataFrame:

        sparql_query = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            SELECT DISTINCT ?authorId ?authorName ?title
            WHERE {{
                {{ 
                    ?work schema:identifier "{input_id}" .
                    ?work schema:name ?title .
                    ?work schema:creator ?author .
                }}
                UNION
                {{ 
                    ?author schema:identifier "{input_id}" .
                    ?work schema:creator ?author .
                    ?work schema:name ?title .
                }}
                OPTIONAL {{ ?author rdfs:label ?authorName . }}
                OPTIONAL {{ ?author schema:name ?authorName . }}
                OPTIONAL {{ ?author schema:identifier ?authorId . }}
            }}
        """
        return self._execute_query(sparql_query)

    def getCulturalHeritageObjectsAuthoredBy(self, input_id: str) -> pd.DataFrame:
        sparql_query = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX schema: <https://schema.org/>

            SELECT DISTINCT ?type_name ?id ?title ?date ?owner ?place ?name ?author_id
            WHERE {{
                {{  ?Author schema:identifier "{input_id}" .
                    ?object schema:creator ?Author .
                    ?object rdf:type ?type .
                    ?object schema:name ?title .
                    ?object schema:identifier ?id .
                    ?object schema:dateCreated ?date .
                    ?object schema:provider ?owner .
                    ?object schema:contentLocation ?place .
                }}
                UNION
                {{  ?entity schema:identifier "{input_id}" .
                    ?entity schema:creator ?Author .
                    ?object schema:creator ?Author .
                    ?object rdf:type ?type .
                    ?object schema:name ?title .
                    ?object schema:identifier ?id .
                    ?object schema:dateCreated ?date .
                    ?object schema:provider ?owner .
                    ?object schema:contentLocation ?place .
                }}
                OPTIONAL {{
                    ?Author rdfs:label ?name .
                    ?Author schema:identifier ?author_id .
                }}

                BIND(REPLACE(STR(?type), "https://schema.org/", "") AS ?type_name)
            }}
        """
        return self._execute_query(sparql_query)
    
    def getAllCulturalHeritageObjectByOwner(self, ownerName: str) -> pd.DataFrame:
        sparql_query = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>

            SELECT DISTINCT ?id ?title ?date ?owner ?place ?author_id ?author_name ?type_name
            WHERE {{
                ?entity rdf:type ?type .
                BIND(REPLACE(STR(?type), "https://schema.org/", "") AS ?type_name)
                FILTER(?type_name != "Author")

                OPTIONAL {{ ?entity schema:name ?title . }}
                OPTIONAL {{ ?entity schema:identifier ?id . }}
                OPTIONAL {{ ?entity schema:dateCreated ?date . }}
                OPTIONAL {{ ?entity schema:provider ?owner . }}
                OPTIONAL {{ ?entity schema:contentLocation ?place . }}

                OPTIONAL {{
                    ?entity schema:creator ?authorEntity .
                    OPTIONAL {{ ?authorEntity schema:identifier ?author_id . }}
                    OPTIONAL {{ ?authorEntity rdfs:label ?author_name . }}
                }}

                FILTER(BOUND(?owner) && REGEX(STR(?owner), "{ownerName}", "i"))
            }}
        """

        return self._execute_query(sparql_query)
    
    def getAllCulturalHeritageObjectCreatedAfter(self, year: int) -> pd.DataFrame:
        sparql_query = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

            SELECT DISTINCT ?date ?id ?title ?owner ?place ?author_id ?author_name ?type_name
            WHERE {{
                ?entity rdf:type ?type .
                BIND(REPLACE(STR(?type), "https://schema.org/", "") AS ?type_name)
                FILTER(?type_name != "Author")

                OPTIONAL {{ ?entity schema:name ?title . }}
                OPTIONAL {{ ?entity schema:identifier ?id . }}
                OPTIONAL {{ ?entity schema:dateCreated ?date . }}
                OPTIONAL {{ ?entity schema:provider ?owner . }}
                OPTIONAL {{ ?entity schema:contentLocation ?place . }}
                OPTIONAL {{
                    ?entity schema:creator ?authorEntity .
                    OPTIONAL {{ ?authorEntity schema:identifier ?author_id . }}
                    OPTIONAL {{ ?authorEntity rdfs:label ?author_name . }}
                }}

                BIND(xsd:integer(REPLACE(STRBEFORE(STR(?date), "-"), "[^0-9]", "")) AS ?startYear)
                BIND(xsd:integer(REPLACE(STRAFTER(STR(?date), "-"), "[^0-9]", "")) AS ?endYear)
                BIND(
                    COALESCE(
                        ?endYear,
                        ?startYear,
                        xsd:integer(REPLACE(STR(?date), "[^0-9]", ""))
                    ) AS ?effectiveEnd
                )

                FILTER(BOUND(?effectiveEnd) && ?effectiveEnd > {year})
            }}
        """
        return self._execute_query(sparql_query)
    
    def getAuthorsOfCulturalHeritageObjectCreatedAfter(self, year: int) -> pd.DataFrame:
        sparql_query = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

            SELECT DISTINCT ?author_name ?author_id
            WHERE {{
                ?entity rdf:type ?type .
                BIND(REPLACE(STR(?type), "https://schema.org/", "") AS ?type_name)
                FILTER(?type_name != "Author")

                OPTIONAL {{ ?entity schema:dateCreated ?date . }}
                OPTIONAL {{ ?entity schema:name ?title . }}
                OPTIONAL {{
                    ?entity schema:creator ?authorEntity .
                    OPTIONAL {{ ?authorEntity schema:identifier ?author_id . }}
                    OPTIONAL {{ ?authorEntity rdfs:label ?author_name . }}
                }}

                BIND(xsd:integer(REPLACE(STRBEFORE(STR(?date), "-"), "[^0-9]", "")) AS ?startYear)
                BIND(xsd:integer(REPLACE(STRAFTER(STR(?date), "-"), "[^0-9]", "")) AS ?endYear)
                BIND(
                    COALESCE(
                        ?endYear,
                        ?startYear,
                        xsd:integer(REPLACE(STR(?date), "[^0-9]", ""))
                    ) AS ?effectiveEnd
                )

                FILTER(BOUND(?effectiveEnd) && ?effectiveEnd > {year})
                FILTER(BOUND(?author_name) && BOUND(?author_id))
            }}
        """
        return self._execute_query(sparql_query)

class ProcessDataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
        self.setDbPathOrUrl(Handler.SQLITE_FILE_PATH)

    def getAllActivities(self) -> pd.DataFrame:
        db_file = self.getDbPathOrUrl()
        try:
            conn = sqlite3.connect(db_file)
            query = """
                SELECT object_id, responsible_institute, responsible_person, technique, tool, start_date, end_date, 'Acquisition' as type FROM Acquisition
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Processing' as type FROM Processing
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Modelling' as type FROM Modelling
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Optimising' as type FROM Optimising
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Exporting' as type FROM Exporting
            """
            df = pd.read_sql_query(query, conn)
            return df
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            return pd.DataFrame()
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    def getActivitiesByResponsibleInstitution(self, institution_str: str) -> pd.DataFrame:
        db_file = self.getDbPathOrUrl()
        try:
            conn = sqlite3.connect(db_file)
            like_param = f"%{institution_str}%"
            query = """
                SELECT object_id, responsible_institute, responsible_person, technique, tool, start_date, end_date, 'Acquisition' as type FROM Acquisition WHERE responsible_institute LIKE ?
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Processing' as type FROM Processing WHERE responsible_institute LIKE ?
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Modelling' as type FROM Modelling WHERE responsible_institute LIKE ?
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Optimising' as type FROM Optimising WHERE responsible_institute LIKE ?
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Exporting' as type FROM Exporting WHERE responsible_institute LIKE ?
            """
            params = (like_param,) * 5
            df = pd.read_sql_query(query, conn, params=params)
            return df
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            return pd.DataFrame()
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    def getActivitiesByResponsiblePerson(self, responsible_person_str: str) -> pd.DataFrame:
        db_file = self.getDbPathOrUrl()
        try:
            conn = sqlite3.connect(db_file)
            like_param = f"%{responsible_person_str}%"
            query = """
                SELECT object_id, responsible_institute, responsible_person, technique, tool, start_date, end_date, 'Acquisition' as type FROM Acquisition WHERE responsible_person LIKE ?
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Processing' as type FROM Processing WHERE responsible_person LIKE ?
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Modelling' as type FROM Modelling WHERE responsible_person LIKE ?
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Optimising' as type FROM Optimising WHERE responsible_person LIKE ?
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Exporting' as type FROM Exporting WHERE responsible_person LIKE ?
            """
            params = (like_param,) * 5
            df = pd.read_sql_query(query, conn, params=params)
            return df
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            return pd.DataFrame()
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    def getActivitiesUsingTool(self, tool_str: str) -> pd.DataFrame:

        db_file = self.getDbPathOrUrl()

        try:
            conn = sqlite3.connect(db_file)
            
            like_param = f"%{tool_str}%"
            params = (like_param,) * 5

            query = """
                SELECT object_id, responsible_institute, responsible_person, technique, tool, start_date, end_date, 'Acquisition' AS activity_type FROM Acquisition WHERE tool LIKE ?
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL AS technique, tool, start_date, end_date, 'Processing' AS activity_type FROM Processing WHERE tool LIKE ?
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL AS technique, tool, start_date, end_date, 'Modelling' AS activity_type FROM Modelling WHERE tool LIKE ?
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL AS technique, tool, start_date, end_date, 'Optimising' AS activity_type FROM Optimising WHERE tool LIKE ?
                UNION ALL
                SELECT object_id, responsible_institute, responsible_person, NULL AS technique, tool, start_date, end_date, 'Exporting' AS activity_type FROM Exporting WHERE tool LIKE ?
            """

            df = pd.read_sql_query(query, conn, params=params)
            return df
            
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            return pd.DataFrame()
            
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    def getActivitiesStartedAfter(self, start_date: str) -> pd.DataFrame:
        db_file = self.getDbPathOrUrl()
        try:
            conn = sqlite3.connect(db_file)
            query = """
            SELECT object_id || '|Acquisition|' || start_date AS id, object_id, responsible_institute, responsible_person, technique, tool, start_date, end_date, 'Acquisition' as type 
            FROM Acquisition 
            WHERE start_date >= ? AND start_date <> ''
            UNION ALL
            SELECT object_id || '|Processing|' || start_date AS id, object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Processing' as type 
            FROM Processing 
            WHERE start_date >= ? AND start_date <> ''
            UNION ALL
            SELECT object_id || '|Modelling|' || start_date AS id, object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Modelling' as type 
            FROM Modelling 
            WHERE start_date >= ? AND start_date <> ''
            UNION ALL
            SELECT object_id || '|Optimising|' || start_date AS id, object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Optimising' as type 
            FROM Optimising
            WHERE start_date >= ? AND start_date <> ''
            UNION ALL
            SELECT object_id || '|Exporting|' || start_date AS id, object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Exporting' as type 
            FROM Exporting 
            WHERE start_date >= ? AND start_date <> ''
            """
            params = (start_date,) * 5
            df = pd.read_sql_query(query, conn, params=params)
            return df
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            return pd.DataFrame()
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    def getActivitiesEndedBefore(self, end_date: str) -> pd.DataFrame:
        db_file = self.getDbPathOrUrl()
        try:
            conn = sqlite3.connect(db_file)
            query = """
            SELECT object_id || '|Acquisition|' || end_date AS id, object_id, responsible_institute, responsible_person, technique, tool, start_date, end_date, 'Acquisition' as type 
            FROM Acquisition 
            WHERE end_date <= ? AND end_date <> ''
            UNION ALL
            SELECT object_id || '|Processing|' || end_date AS id, object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Processing' as type 
            FROM Processing 
            WHERE end_date <= ? AND end_date <> ''
            UNION ALL
            SELECT object_id || '|Modelling|' || end_date AS id, object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Modelling' as type 
            FROM Modelling 
            WHERE end_date <= ? AND end_date <> ''
            UNION ALL
            SELECT object_id || '|Optimising|' || end_date AS id, object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Optimising' as type 
            FROM Optimising
            WHERE end_date <= ? AND end_date <> ''
            UNION ALL
            SELECT object_id || '|Exporting|' || end_date AS id, object_id, responsible_institute, responsible_person, NULL as technique, tool, start_date, end_date, 'Exporting' as type 
            FROM Exporting 
            WHERE end_date <= ? AND end_date <> ''
            """
            params = (end_date,) * 5
            df = pd.read_sql_query(query, conn, params=params)
            return df
            
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            return pd.DataFrame()
            
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    def getAcquisitionsByTechnique(self, technique_str: str) -> pd.DataFrame:

        if not self.db_path:
            return pd.DataFrame()
        
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            query = "SELECT * FROM acquisition WHERE technique LIKE ?"
            df = pd.read_sql_query(query, conn, params=('%' + technique_str + '%',))
            df["type"] = "Acquisition"
            return df
        except sqlite3.Error as e:
            print("SQLite error:", e)
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()


class BasicMashup(object):
    def __init__(self, metadataQuery=None, processQuery=None):
        self.metadataQuery = metadataQuery if metadataQuery is not None else []
        self.processQuery = processQuery if processQuery is not None else []

    def cleanMetadataHandlers(self) -> bool:
        self.metadataQuery.clear()
        return True

    def cleanProcessHandlers(self) -> bool:
        self.processQuery.clear()
        return True

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:
        self.metadataQuery.append(handler)
        return True

    def addProcessHandler(self, handler: ProcessDataQueryHandler) -> bool:
        self.processQuery.append(handler)
        return True

    def getEntityById(self, id_string: str) -> Optional['IdentifiableEntity']:
        for handler in self.metadataQuery:
            df = handler.getById(id_string)
            if df is not None and not df.empty:
                break
        else:
            return None 

        row = df.iloc[0]

        prefix = id_string.split(":")[0].upper() if ":" in id_string else None
        if prefix in ["VIAF", "ULAN"]:
            author_name = row.get("authorName") or row.get("label") or "Unknown"
            return Person(id=id_string, name=author_name)

        cultural_object_uri = row.get("CulturalObject") or id_string
        cultural_object = CulturalHeritageObject(
            id=cultural_object_uri,
            title=row.get("label") or "Unknown",
            date=row.get("date"),
            owner=row.get("owner"),
            place=row.get("place"),
            author_id=row.get("authorId"),
            author_name=row.get("authorName"),
        )

        return cultural_object

    def getAllPeople(self) -> List[Person]:
        all_people: List[Person] = []
        processed_ids = set()

        for handler in self.metadataQuery:
            people_df = handler.getAllPeople()
            if people_df is not None and not people_df.empty:
                for _, row in people_df.iterrows():
                    person_id = row["authorId"]
                    if person_id not in processed_ids:
                        person = Person(id=person_id, name=row["authorName"])
                        all_people.append(person)
                        processed_ids.add(person_id)
        return all_people

    def getAllCulturalHeritageObjects(self) -> List['CulturalHeritageObject']:
        all_objects = []
        processed_ids = set()

        class_map = {
            "NauticalChart": NauticalChart,
            "ManuscriptPlate": ManuscriptPlate,
            "ManuscriptVolume": ManuscriptVolume,
            "PrintedVolume": PrintedVolume,
            "PrintedMaterial": PrintedMaterial,
            "Herbarium": Herbarium,
            "Specimen": Specimen,
            "Painting": Painting,
            "Model": Model,
            "Map": Map
        }
        
        for handler in self.metadataQuery:
            df = handler.getAllCulturalHeritageObjects()
            
            if df is not None and not df.empty:
                for _, row in df.iterrows():

                    obj_identifier = str(row.get("id")) 
                    
                    if not obj_identifier or obj_identifier in processed_ids:
                        continue

                    title = row.get("title", "")
                    date = row.get("date")
                    owner = str(row.get("owner")) if pd.notna(row.get("owner")) else None
                    place = row.get("place")
                    author_id = str(row.get("author_id")) if pd.notna(row.get("author_id")) else None
                    author_name = row.get("author_name") if pd.notna(row.get("author_name")) else None

                    hasAuthor = [Person(author_id, author_name)] if author_id and author_name else None
                    
                    type_name = row.get("type_name")

                    if type_name and type_name in class_map:
                        
                        obj_class = class_map[type_name]
 
                        CulturalObjects = obj_class(
                            id=obj_identifier, 
                            title=title, 
                            date=date, 
                            owner=owner, 
                            place=place, 
                            hasAuthor=hasAuthor )
                            
                        all_objects.append(CulturalObjects)
                        processed_ids.add(obj_identifier)
                            
        return all_objects

    def getAuthorsOfCulturalHeritageObject(
        self, object_id: str
    ) -> List[Person]:  
        authors_list = []

        for metadata_qh in self.metadataQuery:
            authors_df = metadata_qh.getAuthorsOfCulturalHeritageObject(object_id)

            for _, row in authors_df.iterrows():
                author = Person(row["authorId"], row["authorName"])
                authors_list.append(author)

        return authors_list

    def getCulturalHeritageObjectsAuthoredBy(
        self, input_id: str
    ) -> List[CulturalHeritageObject]:  
        objects_list = []
        df = pd.DataFrame()

        if len(self.metadataQuery) > 0:
            df = self.metadataQuery[0].getCulturalHeritageObjectsAuthoredBy(input_id)

        if not df.empty:
            for _, row in df.iterrows():
                title = row["title"]
                date = str(row["date"])
                owner = str(row["owner"])
                place = row["place"]
                author_id = str(row["authorId"]) if "authorId" in df.columns else None
                author_name = (
                    row["authorName"] if "authorName" in df.columns else None
                )

                hasAuthor = None
                if author_id and author_name:
                    hasAuthor = [Person(author_id, author_name)]

                type_name = row["type_name"]
                if type_name == "NauticalChart":
                    obj = NauticalChart(id, title, date, owner, place, hasAuthor)
                elif type_name == "ManuscriptPlate":
                    obj = ManuscriptPlate(id, title, date, owner, place, hasAuthor)
                elif type_name == "ManuscriptVolume":
                    obj = ManuscriptVolume(id, title, date, owner, place, hasAuthor)
                elif type_name == "PrintedVolume":
                    obj = PrintedVolume(id, title, date, owner, place, hasAuthor)
                elif type_name == "PrintedMaterial":
                    obj = PrintedMaterial(id, title, date, owner, place, hasAuthor)
                elif type_name == "Herbarium":
                    obj = Herbarium(id, title, date, owner, place, hasAuthor)
                elif type_name == "Specimen":
                    obj = Specimen(id, title, date, owner, place, hasAuthor)
                elif type_name == "Painting":
                    obj = Painting(id, title, date, owner, place, hasAuthor)
                elif type_name == "Model":
                    obj = Model(id, title, date, owner, place, hasAuthor)
                elif type_name == "Map":
                    obj = Map(id, title, date, owner, place, hasAuthor)
                else:
                    print(f"No class defined for type: {type_name}")
                    continue

                objects_list.append(obj)
    
        return objects_list

    def getAllActivities(self) -> List['Activity']:
        all_activities = []

        if not self.processQuery:
            return all_activities

        activity_classes = {
            "Acquisition": Acquisition,
            "Processing": Processing,
            "Modelling": Modelling,
            "Optimising": Optimising,
            "Exporting": Exporting,
        }

        for handler in self.processQuery:
            df = handler.getAllActivities()
            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                activity_type = row.get("type")
                object_id = str(row.get("object_id"))

                institute = row.get("responsible_institute")
                person = row.get("responsible_person")
                tool = row.get("tool")
                start = row.get("start_date")
                end = row.get("end_date")

                cultural_heritage_object = self.getEntityById(object_id)
                
                activity_params = {
                    "refersTo": cultural_heritage_object,
                    "institute": institute,
                    "person": person,
                    "tool": tool,
                    "start": start,
                    "end": end,
                }

                ObjClass = activity_classes.get(activity_type, Activity)
                if ObjClass is Acquisition:
                    technique = row.get("technique")
                    activity = Acquisition(technique=technique, **activity_params)
                else:
                    activity = ObjClass(**activity_params)

                all_activities.append(activity)

        return all_activities

    def getActivitiesByResponsibleInstitution(self, institute_name: str) -> List['Activity']:
        all_activities = []

        activity_classes = {
            "Acquisition": Acquisition,
            "Processing": Processing,
            "Modelling": Modelling,
            "Optimising": Optimising,
            "Exporting": Exporting,
        }

        for handler in self.processQuery:
            df = handler.getActivitiesByResponsibleInstitution(institute_name)

            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    activity_type = row.get("type")
                    object_id = str(row.get("object_id"))

                    institute = row.get("responsible_institute", None)
                    person = row.get("responsible_person", None)
                    tool = row.get("tool", None)
                    start = row.get("start_date", None)
                    end = row.get("end_date", None)

                    cultural_heritage_object = self.getEntityById(object_id)

                    activity_params = {
                        "refersTo": cultural_heritage_object,
                        "institute": institute,
                        "person": person,
                        "tool": tool,
                        "start": start,
                        "end": end,
                    }

                    ObjClass = activity_classes.get(activity_type, Activity)
                    if ObjClass is Acquisition:
                        technique = row.get("technique", None)
                        activity = Acquisition(technique=technique, **activity_params)
                    else:
                        activity = ObjClass(**activity_params)

                    all_activities.append(activity)

        return all_activities

    def getActivitiesByResponsiblePerson(self, person_name: str) -> List['Activity']:
        all_activities = []

        activity_classes = {
            "Acquisition": Acquisition,
            "Processing": Processing,
            "Modelling": Modelling,
            "Optimising": Optimising,
            "Exporting": Exporting,
        }

        for handler in self.processQuery:
            df = handler.getActivitiesByResponsiblePerson(person_name)

            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    activity_type = row.get("type")
                    object_id = str(row.get("object_id"))

                    institute = row.get("responsible_institute", None)
                    person = row.get("responsible_person", None)
                    tool = row.get("tool", None)
                    start = row.get("start_date", None)
                    end = row.get("end_date", None)

                    cultural_heritage_object = self.getEntityById(object_id)

                    activity_params = {
                        "refersTo": cultural_heritage_object,
                        "institute": institute,
                        "person": person,
                        "tool": tool,
                        "start": start,
                        "end": end,
                    }

                    ObjClass = activity_classes.get(activity_type, Activity)
                    if ObjClass is Acquisition:
                        technique = row.get("technique", None)
                        activity = Acquisition(technique=technique, **activity_params)
                    else:
                        activity = ObjClass(**activity_params)

                    all_activities.append(activity)

        return all_activities

    def getActivitiesUsingTool(self, tool_name: str) -> List[Activity]: 
        all_activities = []

        if len(self.processQuery) == 0:
            return all_activities

        activities_df = self.processQuery[0].getAllActivities()

        for _, row in activities_df.iterrows():
            tool_field = row.get("tool", None)

            if isinstance(tool_field, list):
                tools = [t for t in tool_field if isinstance(t, str)]
            elif isinstance(tool_field, str):
                tools = [tool_field]
            else:
                tools = []

            matches = any(
                tool_name.lower() in t.lower()
                for t in tools
            )

            if not matches:
                continue 

            activity_type = row.get("type")
            object_id = str(row.get("object_id"))
            responsible_person = row.get("responsible_person", None)
            responsible_institute = row.get("responsible_institute", None)
            start_date = row.get("start_date", None)
            end_date = row.get("end_date", None)
            technique = row.get("technique", None)

            cultural_heritage_object = self.getEntityById(object_id)

            activity_params = {
                "refersTo": cultural_heritage_object,
                "institute": responsible_institute,
                "person": responsible_person,
                "tool": tools,
                "start": start_date,
                "end": end_date,
            }

            activity_classes = {
                "Acquisition": Acquisition,
                "Processing": Processing,
                "Modelling": Modelling,
                "Optimising": Optimising,
                "Exporting": Exporting,
            }

            ObjClass = activity_classes.get(activity_type, Activity)
            if ObjClass is Acquisition:
                activity = Acquisition(technique=technique, **activity_params)
            else:
                activity = ObjClass(**activity_params)

            all_activities.append(activity)

        return all_activities

    def getActivitiesStartedAfter(self, date: str) -> List['Activity']:
        all_activities = []
        processed_ids = set()

        activity_classes = {
            "Acquisition": Acquisition,
            "Processing": Processing,
            "Modelling": Modelling,
            "Optimising": Optimising,
            "Exporting": Exporting,
        }

        for handler in self.processQuery:
            activities_df = handler.getActivitiesStartedAfter(date)

            if activities_df is not None and not activities_df.empty:
                for _, row in activities_df.iterrows():
                    activity_id = row.get("id")
                    activity_type = row.get("type")
                    object_id = str(row.get("object_id"))

                    responsible_person = row.get("responsible_person", None)
                    responsible_institute = row.get("responsible_institute", None)
                    tool = row.get("tool", None)
                    start_date = row.get("start_date", None)
                    end_date = row.get("end_date", None)

                    refers_to_obj = self.getEntityById(object_id)

                    activity_params = {
                        "refersTo": refers_to_obj,
                        "institute": responsible_institute,
                        "person": responsible_person,
                        "tool": tool,
                        "start": start_date,
                        "end": end_date,
                    }

                    ObjClass = activity_classes.get(activity_type, Activity)
                    if ObjClass is Acquisition:
                        technique = row.get("technique", None)
                        activity = Acquisition(technique=technique, **activity_params)
                    else:
                        activity = ObjClass(**activity_params)

                    if activity:
                        all_activities.append(activity)
                        processed_ids.add(activity_id)

        return all_activities

    
    def getActivitiesEndedBefore(self, date: str) -> List['Activity']:
        all_activities = []
        processed_ids = set()

        activity_classes = {
            "Acquisition": Acquisition,
            "Processing": Processing,
            "Modelling": Modelling,
            "Optimising": Optimising,
            "Exporting": Exporting,
        }

        for handler in self.processQuery:
            activities_df = handler.getActivitiesEndedBefore(date)

            if activities_df is not None and not activities_df.empty:
                for _, row in activities_df.iterrows():
                    activity_id = row.get("id")
                    activity_type = row.get("type")
                    object_id = str(row.get("object_id"))

                    responsible_person = row.get("responsible_person", None)
                    responsible_institute = row.get("responsible_institute", None)
                    tool = row.get("tool", None)
                    start_date = row.get("start_date", None)
                    end_date = row.get("end_date", None)

                    refers_to_obj = self.getEntityById(object_id)

                    activity_params = {
                        "refersTo": refers_to_obj,
                        "institute": responsible_institute,
                        "person": responsible_person,
                        "tool": tool,
                        "start": start_date,
                        "end": end_date,
                    }

                    ObjClass = activity_classes.get(activity_type, Activity)
                    if ObjClass is Acquisition:
                        technique = row.get("technique", None)
                        activity = Acquisition(technique=technique, **activity_params)
                    else:
                        activity = ObjClass(**activity_params)

                    if activity:
                        all_activities.append(activity)
                        processed_ids.add(activity_id)

        return all_activities

    def getAcquisitionsByTechnique(self, technique: str) -> List[Acquisition]:
        all_acquisitions = []
        processed_ids = set()
        
        activity_classes = {
            "Acquisition": Acquisition, 
            "Processing": Processing, 
            "Modelling": Modelling, 
            "Optimising": Optimising, 
            "Exporting": Exporting,
        }

        for handler in self.processQuery:
            acquisitions_df = handler.getAcquisitionsByTechnique(technique)
            
            if acquisitions_df is not None and not acquisitions_df.empty:
                for _, row in acquisitions_df.iterrows():
                    activity_id = row.get("object_id")
                    
                    if activity_id and activity_id not in processed_ids:
                        cultural_heritage_object = self.getEntityById(str(row.get("object_id")))
                        if isinstance(cultural_heritage_object, Person) or cultural_heritage_object is None:
                            cultural_heritage_object = CulturalHeritageObject(
                                id=activity_id,
                                title="",
                                date=None,
                                owner=None,
                                place=None
                            )

                        activity_type = row.get("type")
                        ObjClass = activity_classes.get(activity_type, Activity)

                        activity_params = {
                            "refersTo": cultural_heritage_object,
                            "institute": row.get("responsible_institute"),
                            "person": row.get("responsible_person"),
                            "tool": row.get("tool"),
                            "start": row.get("start_date"),
                            "end": row.get("end_date"),
                        }

                        if ObjClass is Acquisition:
                            technique_val = row.get("technique", "Unknown")
                            activity = Acquisition(technique=technique_val, **activity_params)
                        else:
                            activity = ObjClass(**activity_params)

                        if isinstance(activity, Acquisition):
                            all_acquisitions.append(activity)
                            processed_ids.add(activity_id)

        return all_acquisitions


class AdvancedMashup(BasicMashup):
    def __init__(self, metadataQuery=None, processQuery=None):
        super().__init__(metadataQuery, processQuery)
        
    def getActivitiesOnObjectsAuthoredBy(self, author_id: str) -> list['Activity']:
        all_activities = []
        processed_ids = set()
  
        activity_classes = {
            "Acquisition": Acquisition, 
            "Processing": Processing, 
            "Modelling": Modelling, 
            "Optimising": Optimising, 
            "Exporting": Exporting,}

        objects_df = self.metadataQuery[0].getCulturalHeritageObjectsAuthoredBy(author_id)
        
        if objects_df is None or objects_df.empty or "id" not in objects_df.columns:
            return []
        
        normalized_object_ids = set(objects_df["id"].astype(str).unique())
        
        for handler in self.processQuery:
            all_activities_df = handler.getAllActivities()
            
            if all_activities_df is None or all_activities_df.empty or "object_id" not in all_activities_df.columns:
                continue

            selected_activities_df = all_activities_df[
                all_activities_df["object_id"].astype(str).isin(normalized_object_ids)]

            for _, row in selected_activities_df.iterrows():
                activity_id = row.get("id") 
                
                if activity_id is None:
                    activity_id = str(row.get("object_id")) + "_" + str(row.get("type"))
                
                activity_type = row.get("type")
                object_id_str = str(row.get("object_id")) 
                
                activity_params = {
                    "refersTo": object_id_str, 
                    "institute": row.get("responsible_institute"),
                    "person": row.get("responsible_person"),
                    "tool": row.get("tool"),
                    "start": row.get("start_date"),
                    "end": row.get("end_date"),
                }
                
                ObjClass = activity_classes.get(activity_type, Activity)
                activity = None
                
                if ObjClass is Acquisition:
                    technique = row.get("technique")
                    activity = Acquisition(technique=technique, **activity_params)
                else:
                    activity = ObjClass(**activity_params)
                
                if activity:
                    all_activities.append(activity)
                    processed_ids.add(activity_id)
                        
        return all_activities

    def getObjectsHandledByResponsiblePerson(self, responsible_person: str) -> List[CulturalHeritageObject]:
        all_object_ids = set()
    
        object_classes = {
            "NauticalChart": NauticalChart,
            "ManuscriptPlate": ManuscriptPlate,
            "ManuscriptVolume": ManuscriptVolume,
            "PrintedVolume": PrintedVolume,
            "PrintedMaterial": PrintedMaterial,
            "Herbarium": Herbarium,
            "Specimen": Specimen,
            "Painting": Painting,
            "Model": Model,
            "Map": Map, }

        for handler in self.processQuery:
            
            activities_df = handler.getActivitiesByResponsiblePerson(responsible_person)

            if activities_df is not None and not activities_df.empty and "object_id" in activities_df.columns:
                all_object_ids.update(activities_df["object_id"].unique().astype(str))
            
        objects_df = self.metadataQuery[0].getAllCulturalHeritageObjects()
        if objects_df is None or objects_df.empty:
            return []
        
        selected_objects_df = objects_df[objects_df["id"].astype(str).isin(all_object_ids)]
        
        result_objects = []
        
        for _, row in selected_objects_df.iterrows():
            object_type = row.get("type_name")
            ObjClass = object_classes.get(object_type, CulturalHeritageObject)
            
            CulturalObjects = ObjClass(
                id=row.get("id"),
                title=row.get("title"),
                author_name=row.get("author_name"),
                author_id=row.get("author_id"),
                date=row.get("date"),
                owner=row.get("owner"),
                place=row.get("place"), )
            
            result_objects.append(CulturalObjects)
                
        return result_objects

    def getObjectsHandledByResponsibleInstitution(self, institute_name: str) -> List[CulturalHeritageObject]:
        all_object_ids = set()
    
        object_classes = {
            "NauticalChart": NauticalChart,
            "ManuscriptPlate": ManuscriptPlate,
            "ManuscriptVolume": ManuscriptVolume,
            "PrintedVolume": PrintedVolume,
            "PrintedMaterial": PrintedMaterial,
            "Herbarium": Herbarium,
            "Specimen": Specimen,
            "Painting": Painting,
            "Model": Model,
            "Map": Map, }

        for handler in self.processQuery:

            activities_df = handler.getActivitiesByResponsibleInstitution(institute_name)
            
            if activities_df is not None and not activities_df.empty and "object_id" in activities_df.columns:
                all_object_ids.update(activities_df["object_id"].unique().astype(str))
            
        objects_df = self.metadataQuery[0].getAllCulturalHeritageObjects()
        selected_objects_df = objects_df[objects_df["id"].astype(str).isin(all_object_ids)]
        
        result_objects = []
        
        for _, row in selected_objects_df.iterrows():
            object_type = row.get("type_name")
            ObjClass = object_classes.get(object_type, CulturalHeritageObject)
            
            CulturalObjects = ObjClass(
                id=row.get("id"),
                title=row.get("title"),
                author_name=row.get("author_name"),
                author_id=row.get("author_id"),
                date=row.get("date"),
                owner=row.get("owner"),
                place=row.get("place"), )
            
            result_objects.append(CulturalObjects)
                
        return result_objects

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start_date: str, end_date: str) -> List[Person]:
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)

        all_started_ids = set()
        all_ended_ids = set()
        
        for handler in self.processQuery:
            all_activities_df = handler.getAllActivities()
                
            all_activities_df['start_dt'] = pd.to_datetime(all_activities_df["start_date"], errors='coerce')
            all_activities_df['end_dt'] = pd.to_datetime(all_activities_df["end_date"], errors='coerce')

            started_acquisitions = all_activities_df[
                (all_activities_df["type"] == "Acquisition") & 
                (all_activities_df["start_dt"] >= start_dt)]
            all_started_ids.update(started_acquisitions["object_id"].unique().astype(str))

            ended_acquisitions = all_activities_df[
                (all_activities_df["type"] == "Acquisition") & 
                (all_activities_df["end_dt"] <= end_dt)]
            all_ended_ids.update(ended_acquisitions["object_id"].unique().astype(str))
                    
        common_ids = all_started_ids.intersection(all_ended_ids)
        #print("IDs of this timeframe (Acquisition started >= start date AND Acquisition ended <= end date):", common_ids)

        all_authors = set()
        objects_df = self.metadataQuery[0].getAllCulturalHeritageObjects()
          
        selected_objects_df = objects_df[objects_df["id"].astype(str).isin(common_ids)]
 
        for _, row in selected_objects_df.iterrows():
            author_name = row.get("author_name")
            author_id = row.get("author_id")
            
            author_id_str = str(author_id)

            if pd.isna(author_name) or author_name is None:
                author_name_str = ""
            else:
                author_name_str = str(author_name)

            person = Person(author_id_str, author_name_str)
            all_authors.add(person)
           
        return list(all_authors)
    
    def getCulturalHeritageObjectsByAuthorAndOwner(self, personId: str, owner: str) -> List[CulturalHeritageObject]:

        self.object_classes = {
            "NauticalChart": NauticalChart,
            "ManuscriptPlate": ManuscriptPlate,
            "ManuscriptVolume": ManuscriptVolume,
            "PrintedVolume": PrintedVolume,
            "PrintedMaterial": PrintedMaterial,
            "Herbarium": Herbarium,
            "Specimen": Specimen,
            "Painting": Painting,
            "Model": Model,
            "Map": Map, }

        owner_df = self.metadataQuery[0].getAllCulturalHeritageObjectByOwner(owner)
        if owner_df is None or owner_df.empty or "id" not in owner_df.columns:
            return []

        owner_df["author_id"] = owner_df["author_id"].fillna("").astype(str)
        owner_df["id"] = owner_df["id"].fillna("").astype(str)
        personId = str(personId)

        filtered_df = owner_df[
            (owner_df["author_id"] == personId) | (owner_df["id"] == personId)
        ]

        if filtered_df.empty:
            return []

        result_objects = []
        for _, row in filtered_df.iterrows():
            object_type = row.get("type_name")
            ObjClass = self.object_classes.get(object_type, CulturalHeritageObject)

            obj_instance = ObjClass(
                id=row.get("id"),
                title=row.get("title"),
                author_name=row.get("author_name"),
                author_id=row.get("author_id"),
                date=row.get("date"),
                owner=row.get("owner"),
                place=row.get("place"),
            )
            result_objects.append(obj_instance)

        return result_objects


GRAPH_ENDPOINT = "http://127.0.0.1:9999/blazegraph/sparql"
CSV_FILE = "data/meta.csv" 
