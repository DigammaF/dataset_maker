
from dataclasses import dataclass, field
from pathlib import Path
import random
from typing import Any, Callable, Literal, Protocol, Self, Sequence

from .utils import get_or_create


type Primary = None | int | str
type Extractor[T] = Callable[[T], Primary]
type Filter[T] = Callable[[T], bool]
type DatabaseRoutine = Callable[[Database], None]

DATABASE: Database | None = None
NO_ID = -1

def anything(value: Any) -> Literal[True]:
	return True

def get_database() -> Database:
	if DATABASE is None:
		raise ValueError("no current database")
	
	return DATABASE

class Entity:
	id = NO_ID

@dataclass
class ForeignKey[T: Entity]:
	type: type[T]
	id: int = field(default=NO_ID)

	@staticmethod
	def of_instance[K: Entity](instance: K) -> ForeignKey[K]:
		return ForeignKey(instance.__class__, instance.id)

	def set(self, instance: T):
		self.id = instance.id

	def get_of_database(self, database: Database) -> Watch[T]:
		return database.get(self.type, self.id)
	
	def get(self) -> Watch[T]:
		return self.get_of_database(get_database())

@dataclass(frozen=True)
class Column[T: Entity]:
	name: str
	extractor: Extractor[T]

class StateNotSuitable(Exception):
	...

@dataclass(frozen=True)
class Database:
	path: Path
	buffer: list[str] = field(default_factory=list[str])
	tables: dict[type[Entity], str] = field(default_factory=dict[type, str])
	ids: dict[type[Entity], int] = field(default_factory=dict[type, int])
	columns: dict[type[Entity], tuple[Column, ...]] = field(default_factory=dict[type, tuple])
	storages: dict[type[Entity], dict[int, Entity]] = field(default_factory=dict)
	max_buffer_size: int = field(default=1_000_000)

	def __enter__(self):
		global DATABASE

		with open(self.path, "w", encoding="utf-8") as file:
			file.write("")

		DATABASE = self

	def __exit__(self, exc_type, exc, tb):
		global DATABASE
		self.flush_buffer()
		DATABASE = None

	def register_type[K: Entity](
		self, type: type[K], table: str, columns: tuple[Column[K], ...]
	):
		self.tables[type] = table
		self.ids[type] = 1
		self.columns[type] = columns
		self.storages[type] = { }

	def get_storage[K](self, type: type[K]) -> dict[int, K]:
		if type in self.storages:
			return self.storages[type] # type: ignore
		
		raise RuntimeError(f"unknown type: {type}")

	def flush_buffer(self):
		with open(self.path, "a", encoding="utf-8") as file:
			for line in self.buffer:
				file.write(f"{line};\n")

		self.buffer.clear()

	def append_buffer(self, line: str):
		self.buffer.append(line)

		if len(self.buffer) > self.max_buffer_size:
			self.flush_buffer()

	def fmt_value(self, value: Primary) -> str:
		if value is None:
			return "null"

		if isinstance(value, str):
			return f"'{value}'"
		
		return str(value)

	def buffer_insert(self, table: str, columns: Sequence[str], values: Sequence[Primary]):
		values = tuple(self.fmt_value(value) for value in values)
		self.append_buffer(
			f"insert into {table} ({','.join(columns)} values ({','.join(values)}))"
		)

	def buffer_update(self, table: str, id: int, map: dict[str, Primary]):
		self.append_buffer(
			f"update {table} set {','.join(f"{column}={self.fmt_value(value)}" for column, value in map.items())} where id={id}"
		)

	def generate_id(self, type: type[Entity]) -> int:
		id = self.ids[type]
		self.ids[type] = id + 1
		return id

	def _create[T: Entity](self, instance: T) -> T:
		instance.id = self.generate_id(instance.__class__)
		columns = self.columns[instance.__class__]
		self.buffer_insert(
			self.tables[instance.__class__],
			[ column.name for column in columns ],
			[ column.extractor(instance) for column in columns ]
		)
		storage = self.get_storage(instance.__class__)
		storage[instance.id] = instance
		return instance
	
	def create[T: Entity](self, factory: Callable[[], T]) -> Watch[T]:
		instance = factory()
		self._create(instance)
		return Watch(instance, self)

	def update(self, instance: Entity):
		map: dict[str, Primary] = { }

		for column in self.columns[instance.__class__]:
			map[column.name] = column.extractor(instance)

		self.buffer_update(
			self.tables[instance.__class__],
			instance.id, map
		)

	def _get[T: Entity](self, type: type[T], id: int) -> T:
		return self.get_storage(type)[id]
	
	def get[T: Entity](self, type: type[T], id: int) -> Watch[T]:
		instance = self._get(type, id)
		return Watch(instance, self)

	def pick[T: Entity](self, type: type[T], filter: Filter[T] = anything) -> Watch[T]:
		storage = self.get_storage(type)
		entities = tuple(entity for entity in storage.values() if filter(entity))

		if entities:
			return Watch(random.choice(entities), self)
		
		raise StateNotSuitable

def create[T: Entity](self, factory: Callable[[], T]) -> Watch[T]:
	return get_database().create(factory)

def get[T: Entity](self, type: type[T], id: int) -> Watch[T]:
	return get_database().get(type, id)

def pick[T: Entity](self, type: type[T], filter: Filter[T] = anything) -> Watch[T]:
	return get_database().pick(type, filter)

@dataclass(frozen=True)
class Watch[T: Entity]:
	instance: T
	database: Database

	def __enter__(self) -> Self:
		return self

	def __exit__(self, exc_type, exc, tb):
		self.database.update(self.instance)

@dataclass
class SimulationStatistics:
	rountine_run_count: int = field(default=0)

@dataclass(frozen=True)
class Simulation:
	routines: tuple[DatabaseRoutine, ...] = field(default_factory=tuple)
	goal: Filter[Simulation] = field(default=anything)
	database: Database = field(default_factory=get_database)
	statistics: SimulationStatistics = field(default_factory=SimulationStatistics)

	def run_routine(self, rountine: DatabaseRoutine):
		try:
			rountine(self.database)

		except StateNotSuitable:
			return
		
		self.statistics.rountine_run_count += 1

	def run_random_routine(self):
		self.run_routine(random.choice(self.routines))

	def run(self):
		while not self.goal(self):
			self.run_random_routine()
