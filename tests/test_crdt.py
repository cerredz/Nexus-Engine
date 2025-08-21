import sys
sys.path.append("engine")
from crdt.crdt import CRDT

def test_crdt():
    crdt1 = CRDT("site1")
    assert crdt1.ROOT_ID in crdt1.atoms

    # Test insert_char
    atom1 = crdt1.insert_char("a", crdt1.ROOT_ID)
    assert atom1.value == "a"
    assert crdt1.atoms[atom1.id] == atom1

    atom2 = crdt1.insert_char("b", atom1.id)
    conv = crdt1.converge()
    assert conv["text"] == "ab"
    assert len(conv["id_mapping"]) == 2

    # Test delete
    crdt1.delete(atom1.id)
    conv = crdt1.converge()
    assert conv["text"] == "b"

    # Multi-site
    crdt2 = CRDT("site2")
    crdt2.insert_atom(atom1)  # simulate sync
    crdt2.insert_atom(atom2)
    crdt2.insert_char("c", atom1.id)  # concurrent insert after a
    conv2 = crdt2.converge()
    assert conv2["text"] == "acb"  # order by timestamp/site_id

    # Tombstone
    crdt2.delete(atom2.id)
    conv2 = crdt2.converge()
    assert "b" not in conv2["text"]

    print("All CRDT tests passed")

if __name__ == "__main__":
    test_crdt()
