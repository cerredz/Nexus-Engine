from engine.crdt.schemas import Atom
from typing import Dict, List
import time 


class CRDT():
    def __init__(self, site_id: str):
        self.atoms: Dict[str, Atom] = {}
        self.site_id = site_id
        root_atom = Atom(
            id="ROOT",
            value=None,
            predecessor_id=None,
            tombstone=False,
            timestamp=0,
            site_id="ROOT",
        )

        self.atoms[root_atom.id] = root_atom
        self.ROOT_ID = root_atom.id

    def generate_unique_id(self) -> str:
        return f"{int(time.time_ns())}-{self.site_id}"
    
    def insert_atom(self, atom: Atom):
        if not isinstance(atom, Atom):
            raise Exception("Error inserting atom in CRDT document, inserted atom is not of type atom")
        self.atoms[atom.id] = atom

    def insert_char(self, char: str, prev_id: str) -> Atom:
        if prev_id not in self.atoms:
            raise ValueError(f"predecessor id {prev_id} not found")
        ts = int(time.time_ns())
        new_id = f"{ts}-{self.site_id}"
        new_atom = Atom(
            id=new_id,
            value=char,
            predecessor_id=prev_id,
            tombstone=False,
            timestamp=ts,
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
            # deterministic total order
            return (a.timestamp, a.site_id, a.id)

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






