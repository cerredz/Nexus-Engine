from typing import Dict, List
import time
from .schemas import Atom

class CRDT():
	def __init__(self, site_id: str):
		self.atoms: Dict[str, Atom] = {}
		self.site_id = site_id
		self._last_ts = 0
		self._seq = 0
		root_atom = Atom(
			id="ROOT",
			value=None,
			predecessor_id=None,
			tombstone=False,
			timestamp=0,
			sequence=0,
			site_id="ROOT",
		)
		self.atoms[root_atom.id] = root_atom
		self.ROOT_ID = root_atom.id

	def _next_id(self):
		ts = int(time.time_ns())
		if ts == self._last_ts:
			self._seq += 1
		else:
			self._last_ts = ts
			self._seq = 0
		new_id = f"{ts}-{self._seq}-{self.site_id}"
		return ts, self._seq, new_id

	def generate_unique_id(self) -> str:
		_, _, new_id = self._next_id()
		return new_id
	
	def insert_atom(self, atom: Atom):
		if not isinstance(atom, Atom):
			raise Exception("Error inserting atom in CRDT document, inserted atom is not of type atom")
		self.atoms[atom.id] = atom

	def insert_char(self, char: str, prev_id: str) -> Atom:
		if prev_id not in self.atoms:
			raise ValueError(f"predecessor id {prev_id} not found")
		ts, seq, new_id = self._next_id()
		new_atom = Atom(
			id=new_id,
			value=char,
			predecessor_id=prev_id,
			tombstone=False,
			timestamp=ts,
			sequence=seq,
			site_id=self.site_id,
		)
		self.atoms[new_atom.id] = new_atom
		return new_atom
	
	def delete(self, atom_id: str):
		existing = self.atoms.get(atom_id)
		if not existing:
			return
		existing.tombstone = True
		self.atoms[atom_id] = existing

	def converge(self):
		children_map: Dict[str, List[Atom]] = {}
		for atom in self.atoms.values():
			if atom.predecessor_id:
				children_map.setdefault(atom.predecessor_id, []).append(atom)

		def sort_key(a: Atom):
			# newer first, break ties with sequence, then site_id, then id
			return (-a.timestamp, -a.sequence, a.site_id, a.id)

		text_result: List[str] = []
		id_map_result: List[str] = []

		def traverse(parent_id: str):
			children = sorted(children_map.get(parent_id, []), key=sort_key)
			for child in children:
				if not child.tombstone and child.value is not None:
					text_result.append(child.value)
					id_map_result.append(child.id)
				traverse(child.id)

		traverse(self.ROOT_ID)
		return {
			"text": "".join(text_result),
			"id_mapping": id_map_result,
		}