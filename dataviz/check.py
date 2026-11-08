"""Smoke test CLI : lit le Data Lake et affiche un indicateur.
Usage : python check.py clients | doublons | statut <SOURCE> <ID>
(ex: python check.py statut ABASSURANCE 1 -> statut courant du client 1)"""
import sys
import app_lib as lib

cmd = sys.argv[1] if len(sys.argv) > 1 else "doublons"
if cmd == "clients":
    print(len(lib.load_clients()))
elif cmd == "doublons":
    print(len(lib.doublons(lib.load_clients())))
elif cmd == "statut":
    source, ident = sys.argv[2], sys.argv[3]
    c = lib.load_clients()
    row = c[(c.source == source) & (c.id_source == ident)]
    print(row.statut.iloc[0] if not row.empty else "ABSENT")
