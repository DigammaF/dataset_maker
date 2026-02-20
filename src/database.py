
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol, Sequence


type Primary = None | int | str
type Extractor = Callable[[Entity], Primary]

DATABASE: Database | None = None
NO_ID = -1

def get_database() -> Database:
	if DATABASE is None:
		raise ValueError("no current database")
	
	return DATABASE

class Entity:
	id = NO_ID

@dataclass(frozen=True)
class Column:
	name: str
	extractor: Extractor

@dataclass(frozen=True)
class Database:
	path: Path
	buffer: list[str] = field(default_factory=list[str])
	tables: dict[type[Entity], str] = field(default_factory=dict[type, str])
	ids: dict[type[Entity], int] = field(default_factory=dict[type, int])
	columns: dict[type[Entity], tuple[Column, ...]] = field(default_factory=dict[type, tuple])

	def __enter__(self):
		global DATABASE

		with open(self.path, "w", encoding="utf-8") as file:
			file.write("")

		DATABASE = self

	def __exit__(self, exc_type, exc, tb):
		global DATABASE
		self.flush_buffer()
		DATABASE = None

	def flush_buffer(self):
		with open(self.path, "a", encoding="utf-8") as file:
			for line in self.buffer:
				file.write(f"{line};\n")

		self.buffer.clear()

	def fmt_value(self, value: Primary) -> str:
		if value is None:
			return "null"

		if isinstance(value, str):
			return f"'{value}'"
		
		return str(value)

	def buffer_insert(self, table: str, columns: Sequence[str], values: Sequence[Primary]):
		values = tuple(self.fmt_value(value) for value in values)
		self.buffer.append(
			f"insert into {table} ({','.join(columns)} values ({','.join(values)}))"
		)

	def buffer_update(self, table: str, id: int, map: dict[str, Primary]):
		self.buffer.append(
			f"update {table} set {','.join(f"{column}={self.fmt_value(value)}" for column, value in map.items())} where id={id}"
		)

	def generate_id(self, type: type[Entity]) -> int:
		id = self.ids.get(type, 1)
		self.ids[type] = id + 1
		return id

	def create[T: Entity](self, instance: T) -> T:
		instance.id = self.generate_id(instance.__class__)
		columns = self.columns[instance.__class__]
		self.buffer_insert(
			self.tables[instance.__class__],
			[ column.name for column in columns ],
			[ column.extractor(instance) for column in columns ]
		)
		return instance

	def update(self, instance: Entity):
		map: dict[str, Primary] = { }

		for column in self.columns[instance.__class__]:
			map[column.name] = column.extractor(instance)

		self.buffer_update(
			self.tables[instance.__class__],
			instance.id, map
		)

@dataclass(frozen=True)
class Watch:
	instance: Entity
	database: Database
