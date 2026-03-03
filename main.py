

from dataclasses import dataclass
from pathlib import Path
import random

from src.database import Column, Database, Entity, ForeignKey, Simulation

from faker import Faker


FAKER = Faker()
DATABASE = Database(Path("dataset.sql"))


@dataclass
class Person(Entity):
	name: str
	age: int

DATABASE.register_type(
	Person, "person",
	(
		Column[Person]("name", lambda person: person.name),
		Column[Person]("age", lambda person: person.age)
	)
)

@dataclass
class Speech(Entity):
	text: str
	person: ForeignKey[Person]

DATABASE.register_type(
	Speech, "speech",
	(
		Column[Speech]("text", lambda speech: speech.text),
		Column[Speech]("person", lambda speech: speech.person.id)
	)
)

def create_person(database: Database):
	database.create(lambda : Person(FAKER.name(), random.randint(18, 25)))

def create_speech(database: Database):
	person = database.pick(Person).instance
	database.create(lambda : Speech(
		FAKER.text(), ForeignKey.of_instance(person)
	))

def age_person(database: Database):
	with database.pick(Person, lambda person: person.age < 33) as person:
		person.instance.age += 1


def main():
	simulation = Simulation(
		(create_person, create_speech, age_person),
		lambda simulation: simulation.statistics.rountine_run_count > 10_000,
		DATABASE
	)
	simulation.run()
	DATABASE.flush_buffer()

if __name__ == "__main__":
	main()
