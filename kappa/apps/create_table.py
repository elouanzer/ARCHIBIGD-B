from cassandra.cluster import Cluster
clstr=Cluster(['172.18.0.3'])
session=clstr.connect()

qry=''' 
CREATE KEYSPACE IF NOT EXISTS projet 
WITH replication = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};'''
	
session.execute(qry) 

qry=''' 
CREATE TABLE IF NOT EXISTS projet.parking_1 (
   date timestamp,
   grp_identifiant text,
   grp_nom text,
   grp_statut int,
   grp_disponible int,
   grp_exploitation int,
   grp_complet int,
   grp_horodatage text,
   idobj text,
   longitude double,
   latitude double,
   disponibilite int,
   dispo_pourcentage double,
   PRIMARY KEY (grp_identifiant, date)  -- 'grp_identifiant' et 'date' ensemble pour garantir l'unicité
);'''

session.execute(qry) 
