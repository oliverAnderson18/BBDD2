from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime, Table, DECIMAL, func
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from datetime import datetime, timedelta

engine = create_engine("sqlite+pysqlite:///sakila_db.sqlite3", echo=False, future=True)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

# tablas N:N
film_actor_table = Table(
    'film_actor',
    Base.metadata,
    Column('actor_id', Integer, ForeignKey('actor.actor_id'), primary_key=True),
    Column('film_id', Integer, ForeignKey('film.film_id'), primary_key=True)
)

film_category_table = Table(
    'film_category',
    Base.metadata,
    Column('film_id', Integer, ForeignKey('film.film_id'), primary_key=True),
    Column('category_id', Integer, ForeignKey('category.category_id'), primary_key=True)
)

# definimos las clases necesarias
class Actor(Base):
    __tablename__ = 'actor'
    
    actor_id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    
    films = relationship("Film", secondary=film_actor_table, back_populates="actors")


class Category(Base):
    __tablename__ = 'category'
    
    category_id = Column(Integer, primary_key=True)
    name = Column(String)
    
    films = relationship("Film", secondary=film_category_table, back_populates="categories")


class Film(Base):
    __tablename__ = 'film'
    
    film_id = Column(Integer, primary_key=True)
    title = Column(String)
    length = Column(Integer)
    rental_rate = Column(DECIMAL(4, 2)) 
    
    actors = relationship("Actor", secondary=film_actor_table, back_populates="films")
    categories = relationship("Category", secondary=film_category_table, back_populates="films")
    
    inventories = relationship("Inventory", back_populates="film")


class Inventory(Base):
    __tablename__ = 'inventory'
    
    inventory_id = Column(Integer, primary_key=True)
    film_id = Column(Integer, ForeignKey('film.film_id'))
    
    film = relationship("Film", back_populates="inventories")
    rentals = relationship("Rental", back_populates="inventory")


class Customer(Base):
    __tablename__ = 'customer'
    
    customer_id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    
    rentals = relationship("Rental", back_populates="customer")


class Rental(Base):
    __tablename__ = 'rental'
    
    rental_id = Column(Integer, primary_key=True)
    rental_date = Column(DateTime)
    return_date = Column(DateTime)
    
    customer_id = Column(Integer, ForeignKey('customer.customer_id'))
    inventory_id = Column(Integer, ForeignKey('inventory.inventory_id'))
    
    customer = relationship("Customer", back_populates="rentals")
    inventory = relationship("Inventory", back_populates="rentals")
    
    
def query_1(cliente_id):

    resultado = session.query(
        Customer.first_name,
        Customer.last_name,
        func.count(Rental.rental_id).label('total_alquileres')
    ).join(
        Rental, Customer.customer_id == Rental.customer_id
    ).filter(
        Customer.customer_id == cliente_id
    ).group_by(
        Customer.customer_id
    ).first()

    if resultado:
        print(f"Nombre: {resultado.first_name} {resultado.last_name}")
        print(f"Total de películas alquiladas: {resultado.total_alquileres}\n")
    else:
        print(f"No se encontraron datos para el cliente con ID {cliente_id}.")

def query_2():
    
    resultado = session.query(
        Film.title, 
        func.count(Actor.actor_id).label("n_actores")
    ).join(
        Film.actors
    ).group_by(
        Film.film_id
    ).having(
        func.count(Actor.actor_id) > 3
    ).order_by(
        func.count(Actor.actor_id).desc()
    )
    
    for fila in resultado:
        print(f"Titulo: {fila.title} |  Num_actores: {fila.n_actores}\n")
    
    else:
        print("Ninguna pelicula cumple la condicion")
        
def query_3(titulo):
    
    resultado = session.query(
        Customer.customer_id, 
        Customer.first_name, 
        Customer.last_name, 
        Rental.rental_date,
        Rental.return_date
    ).join(
        Customer.rentals
    ).join(
        Rental.inventory
    ).join(
        Inventory.film
    ).filter(Film.title == titulo)
    
    no_devueltos = []
    for fila in resultado:
        if fila.return_date == None:
            no_devueltos.append(fila.first_name + " " + fila.last_name)
        print(f"ID del cliente: {fila.customer_id}")
        print(f"Nombre del cliente: {fila.first_name} {fila.last_name}")
        print(f"Inicio del alquiler: {fila.rental_date}")
        print(f"Fin del alquiler: {fila.return_date}\n")
    
    if len(no_devueltos) == 0:
        print("Todos los clientes han devuelto su alquiler")
    else:
        print("Estos clientes no han devuelto el alquiler:")
        for persona in no_devueltos:
            print(persona)


def query_4():
    
    fecha_lim = datetime.now() - timedelta(days=30)
    resultado = session.query(
        Customer.first_name, 
        Customer.last_name, 
        func.max(Rental.return_date).label("ultimo_alquiler")
    ).join(
        Customer.rentals
    ).group_by(
        Customer.customer_id
    ).having(
        func.max(Rental.return_date) < fecha_lim  # Esto es, fechas anteriores a la fecha lim
    )
    
    for fila in resultado:
        print(f"Nombre del cliente: {fila.first_name} {fila.last_name}")
        print(f"Ultima fecha de retorno de alquiler: {fila.ultimo_alquiler}\n")

def query_5():
    
    resultado = session.query(
        Film.title
    ).outerjoin(
        Film.inventories
    ).outerjoin(
        Inventory.rentals
    ).filter(
        Rental.rental_id == None
    )
    
    for fila in resultado:
        print(f"Titulo de la pelicula: {fila.title}\n")
    

def mostrar_menu():
    print("\n--- PRACTICA SAKILA ORM ---")
    print("1. Consultar alquileres por ID de cliente")
    print("2. Peliculas con mas de 3 actores")
    print("3. Datos del cliente por alquiler de pelicula")
    print("4. Clientes que devolvieron alquiler hace mas de 30 dias")
    print("5. Peliculas que nunca fueron alquiladas")
    print("0. Interrumpir el bucle (Salida)")
    print("---------------------------------------------\n")
    

if __name__ == "__main__":
    while True:
        mostrar_menu()
        opcion = input("Selecciona una opcion (0-8): ")

        if opcion == "1":
            try:
                id_cliente = int(input("Introduce el ID del cliente: "))
                query_1(id_cliente)
            except ValueError:
                print("Error: El ID debe ser un numero entero.")

        elif opcion == "2":
            query_2()

        elif opcion == "3":
            try:
                titulo = str(input("Introduce el titulo de la pelicula: "))
                query_3(titulo)
            except ValueError:
                print("Error: El titulo debe ser un string.")
        
        elif opcion == "4":
            query_4()
            
        elif opcion == "5":
            query_5()
            
        elif opcion == "6":
            query_6()
            
        elif opcion == "7":
            query_7()
            
        elif opcion == "8":
            query_8()

        elif opcion == "0":
            print("Salida con exito")
            session.close() 
            break
        
        else:
            print("Opcion no valida. Intentalo de nuevo.")