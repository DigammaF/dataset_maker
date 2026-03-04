
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from os import times
from pathlib import Path
import random
import time

from arrow import Arrow, utcnow

from src.database import Column, Database, Entity, ForeignKey, Not, SameEntity, Simulation, database_iter

from faker import Faker
from rich.progress import Progress, TaskID
from rich.console import Console
from rich.panel import Panel


FAKER = Faker()
DATABASE = Database(Path("dataset.sql"))

# -----------------------------------------------------------------------------

@dataclass
class Categorie(Entity):
	...

DATABASE.register_type(
	Categorie, "Categorie", tuple()
)

@dataclass
class Membre(Entity):
	nom: str = field(default_factory=FAKER.name)
	heures: int = 10

DATABASE.register_type(
	Membre, "Membre", (
		Column[Membre]("nom", lambda membre : membre.nom),
		Column[Membre]("heures", lambda membre : membre.heures)
	)
)

@dataclass
class Transaction(Entity):
	proposition: ForeignKey[Proposition]
	beneficiaire: ForeignKey[Membre]

DATABASE.register_type(
	Transaction, "Transaction", (
		Column[Transaction]("proposition", lambda transaction : transaction.proposition.id),
		Column[Transaction]("beneficiaire", lambda transaction : transaction.beneficiaire.id)
	)
)

@dataclass
class Commentaire(Entity):
	transaction: ForeignKey[Transaction]
	membre: ForeignKey[Membre]
	conversation: int = 0
	parent: ForeignKey[Commentaire] | None = None

DATABASE.register_type(
	Commentaire, "Commentaire", (
		Column[Commentaire]("transaction", lambda commentaire : commentaire.transaction.id),
		Column[Commentaire]("membre", lambda commentaire : commentaire.membre.id),
		Column[Commentaire]("conversation", lambda commentaire : commentaire.conversation),
		Column[Commentaire]("parent", lambda commentaire : commentaire.parent.id if commentaire.parent is not None else None)
	)
)

@dataclass
class Competence(Entity):
	categorie: ForeignKey[Categorie]

DATABASE.register_type(
	Competence, "Competence", (
		Column[Competence]("categorie", lambda competence : competence.categorie.id),
	)
)

@dataclass
class MembreCompetenceRelation(Entity):
	membre: ForeignKey[Membre]
	competence: ForeignKey[Competence]

DATABASE.register_type(
	MembreCompetenceRelation, "MembreCompetenceRelation", (
		Column[MembreCompetenceRelation]("membre", lambda relation : relation.membre.id),
		Column[MembreCompetenceRelation]("competence", lambda relation : relation.competence.id)
	)
)

@dataclass
class MotClef(Entity):
	...

DATABASE.register_type(
	MotClef, "MotClef", tuple()
)

@dataclass
class MembreCompetenceRelationMotClefRelation(Entity):
	membreCompetenceRelation: ForeignKey[MembreCompetenceRelation]
	motClef: ForeignKey[MotClef]

DATABASE.register_type(
	MembreCompetenceRelationMotClefRelation, "MembreCompetenceRelationMotClefRelation", (
		Column[MembreCompetenceRelationMotClefRelation]("membreCompetenceRelation", lambda relation : relation.membreCompetenceRelation.id),
		Column[MembreCompetenceRelationMotClefRelation]("motClef", lambda relation : relation.motClef.id)
	)
)

@dataclass
class Proposition(Entity):
	membre: ForeignKey[Membre]
	competence: ForeignKey[Competence]
	heures: int = 0
	acceptee: bool = False

DATABASE.register_type(
	Proposition, "Proposition", (
		Column[Proposition]("membre", lambda proposition : proposition.membre.id),
		Column[Proposition]("competence", lambda proposition : proposition.competence.id),
		Column[Proposition]("heures", lambda proposition : proposition.heures)
	)
)

# -----------------------------------------------------------------------------

def ajouter_membre(database: Database):
	database.create(Membre)

def ajouter_categorie(database: Database):
	database.create(Categorie)

@dataclass
class PropositionRealisee:
	database: Database

	def __call__(self, proposition: Proposition) -> bool:
		return proposition.acceptee

def ajouter_transaction(database: Database):
	proposition = database.pick(
		Proposition, Not(PropositionRealisee(database))
	).instance

	with proposition.membre.get() as prodigant:
		with database.pick(Membre, Not(SameEntity(prodigant.instance))) as beneficiaire:
			database.create(lambda: Transaction(
				ForeignKey.of_instance(proposition),
				ForeignKey.of_instance(beneficiaire.instance)
			))
			beneficiaire.instance.heures -= proposition.heures
			prodigant.instance.heures += proposition.heures
			proposition.acceptee = True

def ajouter_commentaire_parent(database: Database):
	transaction = database.pick(Transaction).instance
	membre = database.pick(Membre).instance
	database.create(lambda: Commentaire(
		ForeignKey.of_instance(transaction),
		ForeignKey.of_instance(membre)
	))

@dataclass
class CommentaireDeTransaction:
	database: Database
	transaction: Transaction

	def __call__(self, commentaire: Commentaire) -> bool:
		return commentaire.transaction.id == self.transaction.id

def ajouter_commentaire_enfant(database: Database):
	transaction = database.pick(Transaction).instance
	membre = database.pick(Membre).instance
	parent = database.pick(
		Commentaire, CommentaireDeTransaction(database, transaction)
	).instance
	database.create(lambda: Commentaire(
		ForeignKey.of_instance(transaction),
		ForeignKey.of_instance(membre),
		parent=ForeignKey.of_instance(parent)
	))

def ajouter_competence(database: Database):
	categorie = database.pick(Categorie).instance
	database.create(lambda: Competence(
		ForeignKey.of_instance(categorie)
	))

def ajouter_membre_competence_relation(database: Database):
	membre = database.pick(Membre).instance
	competence = database.pick(Competence).instance
	database.create(lambda: MembreCompetenceRelation(
		ForeignKey.of_instance(membre),
		ForeignKey.of_instance(competence)
	))

def ajouter_mot_clef(database: Database):
	database.create(MotClef)

def ajouter_membre_competence_relation_mot_clef_relation(database: Database):
	membre_competence_relation = database.pick(MembreCompetenceRelation).instance
	mot_clef = database.pick(MotClef).instance
	database.create(lambda: MembreCompetenceRelationMotClefRelation(
		ForeignKey.of_instance(membre_competence_relation),
		ForeignKey.of_instance(mot_clef)
	))

def ajouter_proposition(database: Database):
	membre = database.pick(Membre).instance
	competence = database.pick(Competence).instance
	database.create(lambda: Proposition(
		ForeignKey.of_instance(membre),
		ForeignKey.of_instance(competence),
		random.randint(1, 4)
	))

# -----------------------------------------------------------------------------

@dataclass
class Check:
	progress: Progress
	task: TaskID = None # type: ignore
	clock: float = 0
	rountine_count_goal: int = 10_000_000

	def __post_init__(self):
		self.task = self.progress.add_task("Rountine count", total=self.rountine_count_goal)

	def __call__(self, simulation: Simulation) -> bool:
		self.progress.update(self.task, completed=simulation.statistics.rountine_total_count)
		now = time.time()

		if now - self.clock > 30:
			self.clock = now
			self.progress.print(f"Success rate: {simulation.statistics.get_routine_success_rate()}")
			simulation.statistics.reset_routine_batch_size()

		return simulation.statistics.rountine_total_count > self.rountine_count_goal

INITIAL_MEMBER_BATCH_SIZE = 5_000_000

def main():
	random.seed("UwU")
	console = Console()
	start = utcnow()

	try:
		with Progress(refresh_per_second=1, console=console) as progress:
			with DATABASE:
				simulation = Simulation(
					[
						ajouter_categorie,
						ajouter_transaction,
						ajouter_commentaire_parent,
						ajouter_commentaire_enfant,
						ajouter_competence,
						ajouter_membre_competence_relation,
						ajouter_mot_clef,
						ajouter_membre_competence_relation_mot_clef_relation,
						ajouter_proposition
					],
					Check(progress)
				)

				task = progress.add_task("Initial member add", total=INITIAL_MEMBER_BATCH_SIZE)

				for _ in range(INITIAL_MEMBER_BATCH_SIZE):
					ajouter_membre(DATABASE)
					progress.update(task, advance=1)

				simulation.run()

	except KeyboardInterrupt:
		pass

	console.print(Panel.fit(f"Completed in: {utcnow().humanize(start, only_distance=True)}", title="Duration"))

	count_text = "\n".join(
		f"{type.__name__}: {len(entities):,}" for type, entities in DATABASE.storages.items()
	)
	console.print(Panel.fit(count_text, title="Entity count"))

if __name__ == "__main__":
	main()
