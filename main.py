from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
from datetime import datetime

class Nodes:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Se cierra la conexión con la base de datos
        self.driver.close()
        
    def create_graph(self, nodes):
        for nodo in nodes:
            with self.driver.session(database="neo4j") as session:
                # Write transactions allow the driver to handle retries and transient errors
                result = session.execute_write(
                    self._create_and_return_graph, nodes, nodo
                )
                for row in result:
                    print("Created graph: {row}".format(row=row))

    @staticmethod
    def _create_and_return_graph(tx, nodos, nodo):
        caracteristicas = nodos[nodo]

        # convertir a tipo de dato datetime si es posible y si no dejarlo como string
        for key in caracteristicas:
            try:
                # si tiene el formateo adecuado convertir a datetime utilizando datetime.fromisoformat
                caracteristicas[key] = datetime.fromisoformat(str(caracteristicas[key]))

            # si no tiene el formateo adecuado dejarlo como string
            except ValueError:
                pass

        query = """
        MERGE (n:{nodo} {props})
        """.format(nodo=nodo, props=str(caracteristicas).replace("'@", "").replace("@'", ""))
        
        result = tx.run(query)
        try:
            return [{"n": row["n"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    # Crear relacion ACTED_IN con propiedad de rol
    def create_relation_actor(self, name, title, role):
        with self.driver.session(database="neo4j") as session:
            result = session.run("MATCH (a:Actor {name: $name}), (m:Movie {title: $title}) MERGE (a)-[r:ACTED_IN {role: $role}]->(m) RETURN type(r)", name=name, title=title, role=role)
            return [record["type(r)"] for record in result]
    
    # Crear relacion DIRECTED de un director a una pelicula con propiedad rol
    def create_relation_director(self, name, title, role):
        with self.driver.session(database="neo4j") as session:
            result = session.run("MATCH (a:Director {name: $name}), (m:Movie {title: $title}) MERGE (a)-[r:DIRECTED {role: $role}]->(m) RETURN type(r)", name=name, title=title, role=role)
            return [record["type(r)"] for record in result]

    # Crear relacion RATED de un usuario a una pelicula con propiedad rating y timestamp
    def create_relation_rate(self, name, title, rating, timestamp):
        with self.driver.session(database="neo4j") as session:
            result = session.run("MATCH (a:User {name: $name}), (m:Movie {title: $title}) MERGE (a)-[r:RATED {rating: $rating, timestamp: $timestamp}]->(m) RETURN type(r)", name=name, title=title, rating=rating, timestamp=timestamp)
            return [record["type(r)"] for record in result]
            

    # Crear relacion IN_GENRE de una pelicula a un genero
    def create_relation_genre(self, title, genre):
        with self.driver.session(database="neo4j") as session:
            result = session.run("MATCH (a:Movie {title: $title}), (m:Genre {name: $genre}) MERGE (a)-[r:IN_GENRE]->(m) RETURN type(r)", title=title, genre=genre)
            return [record["type(r)"] for record in result]

    # Funcion para encontrar un usuario
    def find_user(self, name):
        with self.driver.session(database="neo4j") as session:
            result = session.run("MATCH (n:User) WHERE n.name = $name RETURN n.name AS name", name=name)
            return [record["name"] for record in result]

    # Funcion para encontrar una pelicula
    def find_movie(self, title):
        with self.driver.session(database="neo4j") as session:
            result = session.run("MATCH (n:Movie) WHERE n.title = $title RETURN n.title AS title", title=title)
            return [record["title"] for record in result]

    # Funcion para encontrar un usuario con su relacion rate a pelicula
    def find_user_movie(self, name, title):
        with self.driver.session(database="neo4j") as session:
            result = session.run("MATCH (n:User)-[r:Rated]->(m:Movie) WHERE n.name = $name AND m.title = $title RETURN n.name AS name, r.rating AS rating, m.title AS title", name=name, title=title)
            return [record["name"] for record in result]

if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = "neo4j+s://338f96bd.databases.neo4j.io:7687"
    user = "neo4j"
    password = "_9x18jd6nSMh0SY3YhA5zQqOvgsIohWE8s76co-EcNc"
    app = Nodes(uri, user, password)

    # nodos = {
    #     "prueba": {
    #         "@name@": "Tom Hanks",
    #         "@tmdbId@": 31
    #     }
    # }


    

    nodos = {
        "Director": {
            "@name@": "Juan Avila",
            "@tmdbId@": 31,
            "@born@": "2023-04-12T10:30:00Z",  
            "@died@": "N/A",
            "@bornIn@": "Concord, California, USA",
            "@url@": "url_peli",
            "@imdbId@": 1124,
            "@bio@": "",
            "@poster@": "posterdetom"
        },
        "User": {
            "@name@": "pedrito",
            "@userId@": 31
        }, 
        "Actor": {
            "@name@": "Tom Hanks",
            "@tmdbId@": 31,
            "@born@": "2023-04-12T10:30:00Z",
            "@died@": "N/A",
            "@bornIn@": "Concord, California, USA",
            "@url@": "porter de tom",
            "@imdbId@": 1124,
            "@bio@": "",
            "@poster@": "Poster de Tom Hanks"
        },
        "Movie": {
            "@title@": "Forest Gump",
            "@tmdbld@": 13,
            "@released@": 1994,
            "@imdbRating@": 0.8,
            "@movieId@": 31,
            "@runtime@": 142,
            "@budget@": 55000000,
            "@year@": 1994,
            "@imdbId@": 13,
            "@revenue@": 677945399,
            "@url@": "poster de Tom Hanks",
            "@poster@": "poster_forrest",
            "@plot@": "Forrest Gump, while not intelligent, has accidentally been present at many historic moments, but his true love, Jenny Curran, eludes him.",
            "@countries@": ["USA"],
            "@imdbVotes@": 1791916,
            "@languagues@": ["English"],

        },
        "Genre": {
            "@name@": "Accion"
        },
        
    }

    app.create_graph(nodos)

    app.create_relation_actor("Tom Hanks", "Forest Gump", "Forest")
    app.create_relation_director("Juan Avila", "Forest Gump", "Director")
    app.create_relation_rate("pedrito", "Forest Gump", 5, 2020)
    app.create_relation_genre("Forest Gump", "Accion")

    

    # print(app.find_user("Juan"))
    # print(app.find_movie(""))
    # print(app.find_user_movie("Juan", "Juegos"))

    app.close()