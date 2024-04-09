# MSPR_DATA

Projet visant à créer un traitement et un traitement pour de la prédiction du prochain président de la république

# Comment lancer le projet ?

Le projet fonctionne sous docker.

Si voue êtes sur window ou mac, il faudra créer un fichier de configuration pour votre conteneur.
Sur ces machines les spec sont limité par défaut et elle empêche Airflow de fonctionner.

Céer un fichier dans :

```bash
C:\Users\<Utillisateur>\.wslconfig
```

🚨🚨 Cette configuration est global pour votre Docker. Si vou souhaiter configurer qu'un seul conteneur, il faut utiliser
le fichier wsl.conf directement present dans le conteneur. Plus
sur [Airflow docker compose](https://learn.microsoft.com/en-us/windows/wsl/wsl-config#wslconf)

Et voici un exemple de configuration :

```bash
# Settings apply across all Linux distros running on WSL 2
[wsl2]

# Limits VM memory to use no more than 4 GB, this can be set as whole numbers using GB or MB
memory=8GB

# Sets the VM to use two virtual processors
processors=4

# Sets amount of swap storage space to 8GB, default is 25% of available RAM
swap=4GB

# Sets swapfile path location, default is %USERPROFILE%\AppData\Local\Temp\swap.vhdx
swapfile=C:\\temp\\wsl-swap.vhdx

# Enable experimental features
[experimental]
sparseVhd=true
```

## Comandes d'initalisation

Lancer la commande suivante pour vous placer dans le répertoire airflow :

```bash
cd airflow
```

Pour initializer les conteneurs airflow, il faut lancer le script :

```bash
docker compose up airflow-init
```

Pour initialiser la base de données Redash, vous lancer cette commande :

```bash
docker-compose run --rm redash create_db
```

Lancer ensuite la commande dans le repertoire airflow :

```bash
docker compose up
```

Vous pouvez verifier que tous vos conteneurs tourenent bien avec la commande :

```bash
docker ps
```

🚨🚨 Il est possible que vos conteneurs ne se lance pas, comme le conteneur postgres car une autre instance utilise déja
ce port. Vous devez arrêter les autres services pour lancer airflow dans ce cas.

## Configuration des bases de données

Des bases de données sont présents dans le projet. Elles sont une capture de l'avancement du projet au 9 avril 2024.
les importer vous evitera de lancer les pipline manuelement car il faut le faire dans un ordre précis. Nous devons
restaurer deux bases, postgres (pour les data de traitement) et redash pour la visualisation.

Les dump des deux base se trouve dans le répertoire ``airflow/dumps``. Vous pouvez vou connecter à l'instance:

- [PgAdmin](http://localhost:5050/browser/)
  - mot de passe : admin ou postgres

Une fois cela fait vous devez vous connecter. Au serveur de base de
données avec les infos suivantes:

- **p**: 172.16.5.10
- **port**:5432
- **user**: airflow
- **password**:airflow
- **database**: airflow

L'IP est normalement fixé. Mais si vous n'arrivez pas à vous connecter à l'adresse, vous pouvez determiner le réseau sur
quelle adresse IP tourne votre base de donnée avec la
commande :

```bash
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' airflow-postgres-1
```

## Configuration de airflow

Vous devriez actuellement être capable de vous à airflow :

- [Airflow](http://localhost:8080/)
  - **user**: airflow
  - **mot de passe** : airflow

Pour que les DAG tournent sans accros il manque des variables indispensables au projet, par exemple la
variable ```AIRFLOW_DB_CONNECTION``` qui contient le string de connection la base de données. Vous avez deux options
pour configurer les variables.
Soit, vous les importez. Pou ce faire:

- ### Accédez à Admin > Variables
  - ### Importation :
    - Cliquez sur le boutton a gauche importer un fichier
    - Choisir le fichier se trouvant dans le répertoire ```airflow/varaiables```
    - Cliquez sur le boutton importer
  - ### Configuration :
    - Cliquez sur le bouton " + " pour ajouter une nouvelle variable.
      - Remplissez les détails de la variable :
        - Nom: **AIRFLOW_DB_CONNECTION**
        - valeur: ```postgresql+psycopg2://{USER}:{PASSWORD}@{IP_BASE_DANS_DOCKER}:5432/{BASE_DE_DONNEE}```
        - description: string de connection a la base de données

# Lancer Redash pour la visualisation:
Vous devriez actuellement être capable de vous à Redash :
- [Redash](http://localhost:5000/)
  - **user**: airflow@gmail.com
  - **mot de passe** : airflow
