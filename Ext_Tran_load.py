from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import requests
import json
from datetime import datetime
import time

# Se connecter à la base de données MongoDB
#url_mongo = "mongodb+srv://pippython012:*****@cluster0.v25z4ax.mongodb.net/?retryWrites=true&w=majority"
url_mongo = "mongodb://0.0.0.0:27018/?directConnection=true"
client = MongoClient(url_mongo, serverSelectionTimeoutMS=5000)
db = client.station


#########################################################
##########################################################

def extraction(url):
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(f"Erreur de requête : {response.status_code}")
        return None


################################################################
################################################################

def transformation(donnee_json):
    # Convertir les timestamps en format de date approprié
    for elt in donnee_json.get("data", []):
        timestamp_str = elt.get("timestamp", "")
        if timestamp_str:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            jour = timestamp.strftime("%Y-%m-%d")
            elt["timestamp"] = jour
            heure = timestamp.strftime("%H:%M:%S")
            elt["heure"] = heure
            
            #### Renommage des champs qui ne sont pas supportés par mongodb (point, $, ...etc)
            mapping = {"T. int.": "T_int", "PM2.5": "PM25"}
            for anc_key, nouv_key in mapping.items():
                if anc_key in elt:
                    elt[nouv_key] = elt.pop(anc_key)
    return donnee_json
    
    

##########################################################################
##########################################################################

def chargement(donnee_transf, stat_hour1, avg_stat_hour1):
    if donnee_transf:
        try:
        # Insertion des données de la liste "heure moyenne par jour" dans la collection stat_hour1 
            list_donnee = donnee_transf.get("data", [])
            stat_hour1.insert_many(list_donnee)
            co_pm_moy = list(db.stat_hour1.aggregate([
    {
        "$group": {
            "_id": "$timestamp",
            "moyCO": {"$avg": "$CO"},
            "moyPM25": {"$avg": "$PM25"}
        }
    },
    {
        "$project": {
            "_id": 0,  # Exclure le champ _id
            "timestamp": "$_id",
            "moyCO": 1,
            "moyPM25": 1
        }
    }
]))


            avg_stat_hour1.insert_many(co_pm_moy)         
            print("Données insérées avec succès dans MongoDB.")
        except Exception as e:
            print(f"Erreur lors de l'insertion des données dans MongoDB : {e}")
    else:
        print("Les données transformées sont vides.")
    # Fermeture de la connexion à MongoDB
    #client.close()


##################################################################################
##################################################################################

url_donnee1 = "https://airqino-api.magentalab.it/v3/getStationHourlyAvg/283164601"
url_donnee2 = "https://airqino-api.magentalab.it/v3/getStationHourlyAvg/283181971"
while True:
    donnee_json = extraction(url_donnee1)
    donnee_transf = transformation(donnee_json)
    if "stat_hour1" in db.list_collection_names() and "avg_stat_hour1" in db.list_collection_names():
        db.stat_hour1.drop()
        db.avg_stat_hour1.drop()
        chargement(donnee_transf, db.stat_hour1, db.avg_stat_hour1)
    else:
        chargement(donnee_transf, db.create_collection("stat_hour1"), db.avg_stat_hour1)
        
    donnee_json2 = extraction(url_donnee2)
    donnee_transf2 = transformation(donnee_json2)
    if "stat_hour2" in db.list_collection_names() and "avg_stat_hour2" in db.list_collection_names():
        db.stat_hour2.drop()
        db.avg_stat_hour2.drop()
        chargement(donnee_transf2, db.stat_hour2, db.avg_stat_hour2)
    else:
        chargement(donnee_transf2, db.stat_hour2, db.create_collection("avg_stat_hour2"))

    # Attente d'une heures avant la prochaine exécution
    time.sleep(1 * 60 * 60)


