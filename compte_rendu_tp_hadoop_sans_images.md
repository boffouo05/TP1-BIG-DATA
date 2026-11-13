# Compte rendu — TP Hadoop sous Ubuntu

**Clusters, HDFS, YARN, MapReduce et programmation en Python**

| Élément | Détail |
|---|---|
| Nom / Prénom | Kouadio Yao Boffouo Siedou |
| Formation | Licence — Université Félix Houphouët-Boigny (UFR-MI) |
| Environnement | VM VirtualBox `ubuntu-srv` : Ubuntu Server 26.04.1 LTS, 4 Go de RAM, disque de 25 Go |
| Logiciels | OpenJDK 11, OpenSSH, Hadoop 3.3.6, Python 3 |
| Mode de déploiement | Pseudo-distribué (tous les démons sur une seule machine), utilisateur `hadoop` |
| Dates des manipulations | 1er au 12 novembre 2026 |

---

## Objectif du TP (rappel)

Ce TP met en pratique, sur une machine réelle, les notions de **cluster**, de **stockage distribué (HDFS)**, de **gestion des ressources (YARN)** et du modèle de programmation **MapReduce**, qui seront approfondies au chapitre 3.

À l'issue du TP, il faut être capable de :

1. **Définir** ce qu'est un cluster informatique et en connaître les grandes familles.
2. **Comprendre** l'architecture d'Hadoop : HDFS (stockage) et YARN (calcul).
3. **Installer et configurer** Hadoop en mode pseudo-distribué sur Ubuntu.
4. **Manipuler** HDFS en ligne de commande.
5. **Expliquer** MapReduce phase par phase.
6. **Écrire, tester et exécuter** des programmes MapReduce en Python avec Hadoop Streaming.

**Pourquoi c'est important :** quand les données dépassent ce qu'une seule machine peut stocker ou traiter, on répartit données et calculs sur plusieurs machines. Les concepts vus ici (HDFS, YARN, MapReduce) se retrouvent dans Spark, Hive et les services cloud (AWS EMR, Azure HDInsight, Google Dataproc). Le vrai défi de MapReduce n'est pas la syntaxe mais le **choix de la clé**, qui détermine le regroupement des données.

## Sommaire

1. [Partie 1 — Les clusters](#partie-1--les-clusters)
2. [Partie 2 — Installation d'Hadoop](#partie-2--installation-dhadoop-en-mode-pseudo-distribué)
3. [Partie 3 — Manipulation de HDFS](#partie-3--manipulation-de-hdfs)
4. [Partie 4 — Le modèle MapReduce](#partie-4--le-modèle-mapreduce)
5. [Partie 5 — MapReduce en Python](#partie-5--mapreduce-en-python)
6. [Partie 6 — Dépannage et passage à un vrai cluster](#partie-6--dépannage-et-passage-à-un-vrai-cluster)
7. [Questions de synthèse](#questions-de-synthèse)
8. [Éléments à compléter](#éléments-à-compléter-avant-remise)

---

## Partie 1 — Les clusters

Un **cluster** est un ensemble d'ordinateurs indépendants (les **nœuds**), reliés par un réseau rapide et coordonnés par un logiciel, qui fonctionnent comme un seul système aux yeux de l'utilisateur. Plutôt que d'acheter une machine unique toujours plus puissante, on assemble de nombreuses machines ordinaires.

| Besoin | Apport du cluster |
|---|---|
| Volume de données | Fichiers répartis sur de nombreux disques |
| Puissance de calcul | Traitements en parallèle |
| Haute disponibilité | Si un nœud tombe, les autres prennent le relais |
| Évolutivité | On ajoute des nœuds selon les besoins |
| Coût | Matériel standard (commodity hardware) |

**Scalabilité verticale (scale-up)** : rendre une machine plus puissante ; plafond rapidement atteint, coût qui explose, point unique de défaillance. **Scalabilité horizontale (scale-out)** : ajouter des machines ; quasi illimitée, coût linéaire, tolérance aux pannes. **Hadoop exploite la scalabilité horizontale.**

**Familles de clusters :** haute disponibilité (HA), répartition de charge, calcul haute performance (HPC), et stockage/traitement de données (Big Data), famille d'Hadoop.

**Architecture maître / esclaves :**

- **Nœud maître** : NameNode (métadonnées HDFS) et ResourceManager (ressources YARN).
- **Nœuds esclaves** : DataNode (blocs de données) et NodeManager (conteneurs qui exécutent les tâches Map/Reduce).

**Les quatre modules d'Hadoop :** Hadoop Common, HDFS, YARN, MapReduce.

**HDFS** découpe chaque fichier en blocs de 128 Mo (par défaut), chacun étant répliqué (facteur 3 en production). Le **NameNode** gère l'espace de noms et sait où est chaque bloc ; les **DataNodes** stockent les blocs et envoient des signaux de vie (heartbeats) ; le **Secondary NameNode** fusionne périodiquement le journal `edits` avec l'image `fsimage` (ce n'est pas un NameNode de secours).

**YARN** : le **ResourceManager** arbitre les ressources du cluster, chaque **NodeManager** lance et surveille les conteneurs, l'**ApplicationMaster** pilote les tâches de chaque job.

**Modes de déploiement :** local (standalone), **pseudo-distribué** (utilisé dans ce TP), totalement distribué (production).

---

## Partie 2 — Installation d'Hadoop en mode pseudo-distribué

### 2.0 Préparation de la machine virtuelle

La VM est créée depuis l'ISO d'Ubuntu Server 26.04.1 LTS. Le TP prévoyait Ubuntu 22.04 ou 24.04 : la disponibilité d'OpenJDK 11 sur 26.04 a donc été vérifiée (`apt-cache policy openjdk-11-jdk` : version candidate `11.0.32.1`).

**Mémoire :** 4096 Mo, la valeur demandée par le TP.

**Disque :** le disque d'origine (12 Go dont 4,5 Go libres) était trop juste (archive Hadoop de 700 Mo, décompression, données HDFS). Il a été agrandi à **25 Go** dans VirtualBox, puis le volume LVM a été étendu côté Ubuntu : `sudo pvresize /dev/sda3` puis `sudo lvextend -r -l +100%FREE /dev/ubuntu-vg/ubuntu-lv`.

Après `lvextend`, `df -h /` affiche environ **16 Go disponibles**.

### 2.1 Java

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y openjdk-11-jdk ssh pdsh wget python3
java -version
```

### 2.2 Utilisateur dédié

```bash
sudo adduser hadoop
su - hadoop
```

Toute la suite du TP se fait avec le compte `hadoop` (invite `hadoop@ubuntu-srv:~$`).

### 2.3 SSH sans mot de passe

Les scripts de démarrage d'Hadoop se connectent en SSH à chaque nœud, y compris `localhost`.

```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ssh localhost
```

### 2.4 Téléchargement et installation d'Hadoop 3.3.6

```bash
cd ~
wget -c https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 hadoop
```

L'archive fait 697 Mo. Les erreurs rencontrées à cette étape sont détaillées en partie 6.

### 2.5 Variables d'environnement

Ajoutées à la fin de `~/.bashrc` :

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME HADOOP_YARN_HOME=$HADOOP_HOME PDSH_RCMD_TYPE=ssh
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

Sans cet enregistrement dans `~/.bashrc`, les variables disparaissent à la fermeture de la session et `hadoop` redonne `command not found`.

### 2.6 Fichiers de configuration

Tous situés dans `$HADOOP_HOME/etc/hadoop/` ; chaque fichier a été **remplacé** par son contenu complet (`cat > fichier << 'EOF'`) pour éviter une seconde balise `<configuration>`.

| Fichier | Rôle | Valeurs principales |
|---|---|---|
| `hadoop-env.sh` | Chemin de Java | `JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64` |
| `core-site.xml` | Adresse du système de fichiers | `fs.defaultFS = hdfs://localhost:9000`, `hadoop.tmp.dir = /home/hadoop/hadoopdata/tmp` |
| `hdfs-site.xml` | Paramètres HDFS | `dfs.replication = 1` (une seule machine), dossiers `namenode` et `datanode` |
| `mapred-site.xml` | MapReduce sur YARN | `mapreduce.framework.name = yarn`, `HADOOP_MAPRED_HOME` transmis aux tâches |
| `yarn-site.xml` | Gestionnaire de ressources | service `mapreduce_shuffle` ; mémoire du NodeManager ramenée à 2048 Mo (la VM n'a que ~3,3 Go utilisables) |

Un réglage supplémentaire a été nécessaire : `yarn.app.mapreduce.am.resource.mb = 1024` dans `mapred-site.xml`. Avec 2048 Mo pour YARN, un ApplicationMaster de 1536 Mo (valeur par défaut) plus une tâche Map de 1024 Mo dépasseraient la mémoire disponible et le job resterait bloqué à « map 0% ».

### 2.7 Formatage de HDFS et démarrage du cluster

```bash
mkdir -p ~/hadoopdata/{namenode,datanode,tmp}
hdfs namenode -format      # UNE SEULE FOIS
start-dfs.sh
start-yarn.sh
jps
```

Au premier démarrage, `jps` n'affichait que **4 démons** : le **DataNode était absent**.

**Diagnostic.** Le log du DataNode (`~/hadoop/logs/hadoop-hadoop-datanode-ubuntu-srv.log`) contient `Incompatible clusterIDs` : à chaque formatage, le NameNode reçoit un nouvel identifiant de cluster, alors que le DataNode conserve celui d'un formatage précédent et refuse de démarrer.

**Correction** (la HDFS était encore vide, rien n'a été perdu ; on ne reformate **pas**) :

```bash
stop-yarn.sh
stop-dfs.sh
rm -rf ~/hadoopdata/datanode/*
start-dfs.sh
start-yarn.sh
jps
```

### 2.8 Interfaces web

| Interface | Adresse | Contenu |
|---|---|---|
| NameNode (HDFS) | `http://localhost:9870` | État de HDFS, DataNodes actifs, explorateur de fichiers |
| ResourceManager (YARN) | `http://localhost:8088` | Applications en cours et terminées, ressources du cluster |
| DataNode | `http://localhost:9864` | Informations sur le nœud de stockage |

La VM étant un Ubuntu Server (sans interface graphique), les interfaces sont ouvertes depuis le navigateur de la machine hôte grâce à la **redirection de ports de VirtualBox** (9870 → 9870, 8088 → 8088, 9864 → 9864).

---

## Partie 3 — Manipulation de HDFS

### Exercice 1 — Premiers pas avec HDFS

```bash
hdfs dfs -mkdir -p /user/hadoop/tp/input
mkdir -p ~/tp && cd ~/tp
cat > poeme.txt << 'EOF'
le chat dort sur le tapis
le chien court dans le jardin
le chat court apres le chien
EOF
hdfs dfs -put poeme.txt /user/hadoop/tp/input/
hdfs dfs -ls /user/hadoop/tp/input
hdfs dfs -cat /user/hadoop/tp/input/poeme.txt
hdfs fsck /user/hadoop/tp/input/poeme.txt -files -blocks -locations
```

**Questions du TP**

**1) Combien de blocs `poeme.txt` occupe-t-il ? Pourquoi ?**
**Un seul bloc** (`Total blocks (validated): 1`, taille moyenne 88 B). Le fichier fait 88 octets, très en dessous de la taille d'un bloc HDFS (128 Mo par défaut) : un fichier n'est découpé que s'il dépasse cette taille. Le bloc est une limite logique : il n'occupe sur le disque que les 88 octets réels, pas 128 Mo.

**2) Retrouver le fichier dans l'interface web du NameNode.**
`http://localhost:9870` → **Utilities** → **Browse the file system** → `/user/hadoop/tp/input` : `poeme.txt` apparaît avec une taille de 88 B et une réplication de 1.

**3) Nombre de blocs d'un fichier de 1 Go, et espace disque avec une réplication de 3 ?**
1 Go = 1024 Mo et 1024 / 128 = **8 blocs**. Avec une réplication de 3, le cluster stocke 8 × 3 = **24 blocs**, soit **3 Go** d'espace disque. (Dans cette installation, la réplication est de 1 : le même fichier occuperait 1 Go.)

---

## Partie 4 — Le modèle MapReduce

MapReduce permet de traiter de gros volumes en parallèle. Le développeur n'écrit que deux fonctions ; Hadoop gère le reste (découpage, distribution, transfert des résultats intermédiaires, reprise sur panne).

- `map(clé, valeur) → liste de (clé′, valeur′)`
- `reduce(clé′, liste de valeurs′) → liste de (clé′′, valeur′′)`

| # | Phase | Qui ? |
|---|---|---|
| 1 | Input / Split : découpage du fichier en splits (un par bloc) | Hadoop |
| 2 | **Map** : chaque ligne devient des paires (clé, valeur), en parallèle près des données | **Développeur** |
| 3 | **Combine** (facultatif) : mini-réduction locale pour diminuer le trafic réseau | **Développeur** |
| 4 | Partition : chaque clé est affectée à un reducer (hachage modulo le nombre de reducers) | Hadoop |
| 5 | Shuffle & Sort : transfert, tri et regroupement par clé | Hadoop |
| 6 | **Reduce** : agrégation des valeurs de chaque clé | **Développeur** |
| 7 | Output : un fichier `part-XXXXX` par reducer dans HDFS | Hadoop |

**Exemple WordCount** (« le chat dort », « le chien court », « le chat court ») : Map → `(le,1) (chat,1) (dort,1)…` ; Shuffle & Sort → `chat→[1,1]`, `le→[1,1,1]`… ; Reduce → `(chat,2) (chien,1) (court,2) (dort,1) (le,3)`.

**Avantages :** passage à l'échelle quasi linéaire, tolérance aux pannes automatique, parallélisation gérée par Hadoop, matériel standard. **Limites :** lourd pour les traitements itératifs (écriture disque entre jobs), latence élevée, pas adapté au temps réel ; remplacé en partie par Spark.

---

## Partie 5 — MapReduce en Python

### 5.1 Hadoop Streaming

Hadoop Streaming permet d'écrire le mapper et le reducer dans n'importe quel langage lisant `stdin` et écrivant sur `stdout`. Le mapper écrit des lignes `clé<TAB>valeur`, Hadoop les trie par clé, le reducer reçoit les lignes triées (les lignes d'une même clé sont consécutives) et écrit le résultat.

**Débogage :** un job complet se simule sans Hadoop avec `cat fichier | ./mapper.py | sort | ./reducer.py`. Ce test local est fait avant chaque lancement sur le cluster.

### Exercice 2 — WordCount en Python

**Clé = le mot, valeur = 1 ; le reducer additionne.**

`mapper.py`

```python
#!/usr/bin/env python3
"""mapper.py : émet (mot, 1) pour chaque mot lu sur l'entrée standard."""
import sys
import re

# Hadoop envoie le fichier ligne par ligne sur stdin
for ligne in sys.stdin:
    # on retire les espaces autour et on passe en minuscules
    ligne = ligne.strip().lower()
    # on ne garde que les lettres (accents compris) : cela élimine la ponctuation
    mots = re.findall(r"[a-zàâäéèêëîïôöùûüç]+", ligne)
    for mot in mots:
        # format Streaming : clé<TAB>valeur
        print(f"{mot}\t1")
```

`reducer.py`

```python
#!/usr/bin/env python3
"""reducer.py : additionne les compteurs de chaque mot.
Les lignes arrivent triées par clé : les mêmes mots sont consécutifs."""
import sys

mot_courant = None   # mot en cours de traitement
total = 0            # somme des compteurs du mot en cours

for ligne in sys.stdin:
    mot, valeur = ligne.rstrip("\n").split("\t", 1)
    try:
        valeur = int(valeur)
    except ValueError:
        continue                     # ligne mal formée : on l'ignore

    if mot == mot_courant:
        total += valeur              # même mot : on cumule
    else:
        if mot_courant is not None:  # changement de mot : on émet le précédent
            print(f"{mot_courant}\t{total}")
        mot_courant = mot            # on démarre le nouveau mot
        total = valeur

# ne pas oublier le dernier mot !
if mot_courant is not None:
    print(f"{mot_courant}\t{total}")
```

**Test local puis exécution sur Hadoop**

```bash
cd ~/tp && chmod +x mapper.py reducer.py
cat poeme.txt | ./mapper.py | sort | ./reducer.py     # test local

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input /user/hadoop/tp/input -output /user/hadoop/tp/output_wordcount

hdfs dfs -cat /user/hadoop/tp/output_wordcount/part-00000
```

**Résultat obtenu (identique à celui attendu) :** apres 1, chat 2, chien 2, court 2, dans 1, dort 1, jardin 1, **le 6**, sur 1, tapis 1.

**Lecture des compteurs :** 3 lignes en entrée, 18 mots émis par le Map, regroupés en 10 clés (mots distincts) ; `Combine input records = 0` (pas de combiner) ; 1 reducer, donc un seul fichier `part-00000`. Le log annonce 2 splits : Hadoop Streaming demande par défaut 2 tâches Map, même pour un fichier de 88 octets.

> **À compléter** — *Test sur un vrai texte (livre) avec et sans combiner* : voir la section « Éléments à compléter ».

### Exercice 3 — Chiffre d'affaires par ville

**Clé = la ville, valeur = le montant d'une vente (quantité × prix).** Entrée : `ventes.csv` (`date,ville,produit,quantite,prix_unitaire`).

`mapper_ventes.py`

```python
#!/usr/bin/env python3
"""mapper_ventes.py : émet (ville, montant) pour chaque vente."""
import sys

for ligne in sys.stdin:
    ligne = ligne.strip()
    if not ligne or ligne.startswith("date"):   # ignorer les lignes vides et l'en-tête
        continue
    champs = ligne.split(",")
    if len(champs) != 5:                        # ligne mal formée
        continue
    _, ville, _, quantite, prix = champs        # on ne garde que ville, quantité, prix
    try:
        montant = int(quantite) * float(prix)   # montant de la vente
    except ValueError:
        continue
    print(f"{ville}\t{montant}")                # clé = ville
```

`reducer_ventes.py`

```python
#!/usr/bin/env python3
"""reducer_ventes.py : total et nombre de ventes par ville."""
import sys

ville_courante, total, nb = None, 0.0, 0

def emettre(ville, total, nb):
    # ville, chiffre d'affaires total, nombre de ventes
    print(f"{ville}\t{total:.0f}\t{nb}")

for ligne in sys.stdin:
    ville, montant = ligne.rstrip("\n").split("\t")
    montant = float(montant)
    if ville == ville_courante:
        total += montant                 # même ville : on cumule
        nb += 1
    else:
        if ville_courante is not None:   # nouvelle ville : on émet la précédente
            emettre(ville_courante, total, nb)
        ville_courante, total, nb = ville, montant, 1

# ne pas oublier la dernière ville !
if ville_courante is not None:
    emettre(ville_courante, total, nb)
```

| Ville | CA total (FCFA) | Nombre de ventes |
|---|---:|---:|
| Abidjan | 910000 | 3 |
| Bouake | 445000 | 2 |
| Korhogo | 40000 | 1 |
| San-Pedro | 260000 | 2 |
| Yamoussoukro | 540000 | 2 |

Le résultat est identique à celui attendu et **cohérent avec les compteurs** : 11 lignes lues (en-tête + 10 ventes), 10 paires émises (l'en-tête est ignoré), 5 villes distinctes.

### Exercice 4 — Température maximale par année

**Clé = l'année ; le reducer garde le maximum au lieu de faire une somme.** Entrée : `meteo.txt` (`annee,station,temperature`).

`mapper_meteo.py`

```python
#!/usr/bin/env python3
"""mapper_meteo.py : émet (annee, temperature) pour chaque mesure."""
import sys

for ligne in sys.stdin:
    ligne = ligne.strip()
    if not ligne:
        continue                       # ignorer les lignes vides
    champs = ligne.split(",")
    if len(champs) != 3:
        continue                       # ligne mal formée
    annee, station, temperature = champs
    try:
        float(temperature)             # vérifie que c'est bien un nombre
    except ValueError:
        continue                       # ignore en-tête ou valeur invalide
    print(f"{annee}\t{temperature}")   # clé = année
```

`reducer_meteo.py`

```python
#!/usr/bin/env python3
"""reducer_meteo.py : garde la température maximale de chaque année.
Les lignes arrivent triées par clé : les mêmes années sont consécutives."""
import sys

annee_courante = None
maximum = None

for ligne in sys.stdin:
    annee, valeur = ligne.rstrip("\n").split("\t")
    try:
        valeur = float(valeur)         # conversion numérique (sinon comparaison de texte)
    except ValueError:
        continue                       # ligne mal formée : on l'ignore

    if annee == annee_courante:
        if valeur > maximum:           # on garde le max, pas la somme
            maximum = valeur
    else:
        if annee_courante is not None: # nouvelle année : on émet la précédente
            print(f"{annee_courante}\t{maximum}")
        annee_courante = annee
        maximum = valeur

# ne pas oublier la dernière année !
if annee_courante is not None:
    print(f"{annee_courante}\t{maximum}")
```

Compteurs : Map input records = 6, Map output records = 6, Reduce input groups = 2, Reduce input records = 6, Reduce output records = 2. `Launched map tasks = 2` et `Data-local map tasks = 2` : les deux tâches Map se sont exécutées sur le nœud qui stocke les données (**localité des données**).

**Avec combiner** (`-combiner "python3 reducer_meteo.py"`) : le résultat reste identique, mais `Combine input records = 6`, `Combine output records = 4` et **`Reduce input records` passe de 6 à 4**. Le maximum est associatif et commutatif, et le format de sortie du reducer est identique à celui du mapper, donc il peut servir de combiner. On obtient 4 et non 2 car chacune des 2 tâches Map a son propre combiner, qui émet un maximum par année vue.

### Exercice 5 — Moyenne des notes par matière

**Clé = la matière.** Entrée : `notes.csv` (`etudiant,matiere,note`).

**Question : peut-on utiliser le reducer de moyenne comme combiner ? Justifiez.**
**Non.** La moyenne des moyennes partielles n'est pas égale à la moyenne globale dès que les groupes n'ont pas le même effectif. Exemple : un Map voit les notes 10 et 20 (moyenne 15) et un autre Map voit la note 6 (moyenne 6) ; la moyenne des moyennes vaut (15 + 6) / 2 = 10,5, alors que la vraie moyenne vaut (10 + 20 + 6) / 3 = **12**.

**Solution :** le mapper émet `matiere<TAB>note<TAB>1`. On fait circuler des couples (**somme, nombre**), qui sont additifs. Le combiner additionne les sommes et les nombres **sans diviser** et garde le même format que le mapper ; seul le reducer final divise, une seule fois. Le reducer fonctionne donc avec ou sans combiner.

`mapper_notes.py`

```python
#!/usr/bin/env python3
"""mapper_notes.py : émet (matiere, note, 1) pour chaque note.
Le 1 permet de compter le nombre de notes, ce qui rend les
valeurs additives (utile pour le combiner)."""
import sys

for ligne in sys.stdin:
    ligne = ligne.strip()
    if not ligne or ligne.startswith("etudiant"):   # ignorer l'en-tête
        continue
    champs = ligne.split(",")
    if len(champs) != 3:
        continue                                    # ligne mal formée
    _, matiere, note = champs
    try:
        float(note)                                 # vérifie que c'est un nombre
    except ValueError:
        continue
    print(f"{matiere}\t{note}\t1")                  # clé = matière
```

`combiner_notes.py`

```python
#!/usr/bin/env python3
"""combiner_notes.py : additionne les sommes et les nombres par matière.
Il NE calcule PAS la moyenne : il garde le format matiere<TAB>somme<TAB>nombre,
pour que le reducer puisse ensuite le recevoir comme une sortie de mapper."""
import sys

matiere_courante, somme, nb = None, 0.0, 0

for ligne in sys.stdin:
    champs = ligne.rstrip("\n").split("\t")
    if len(champs) != 3:
        continue
    matiere = champs[0]
    try:
        valeur, n = float(champs[1]), int(champs[2])
    except ValueError:
        continue

    if matiere == matiere_courante:
        somme += valeur                # on additionne les sommes
        nb += n                        # et les nombres
    else:
        if matiere_courante is not None:
            print(f"{matiere_courante}\t{somme}\t{nb}")
        matiere_courante, somme, nb = matiere, valeur, n

if matiere_courante is not None:
    print(f"{matiere_courante}\t{somme}\t{nb}")
```

`reducer_notes.py`

```python
#!/usr/bin/env python3
"""reducer_notes.py : moyenne des notes par matière.
Reçoit matiere<TAB>somme<TAB>nombre (venant du mapper ou du combiner),
additionne, puis divise une seule fois à la fin."""
import sys

matiere_courante, somme, nb = None, 0.0, 0

def emettre(matiere, somme, nb):
    print(f"{matiere}\t{somme / nb:.2f}")   # la division n'a lieu qu'ici

for ligne in sys.stdin:
    champs = ligne.rstrip("\n").split("\t")
    if len(champs) != 3:
        continue
    matiere = champs[0]
    try:
        valeur, n = float(champs[1]), int(champs[2])
    except ValueError:
        continue

    if matiere == matiere_courante:
        somme += valeur
        nb += n
    else:
        if matiere_courante is not None:
            emettre(matiere_courante, somme, nb)
        matiere_courante, somme, nb = matiere, valeur, n

# ne pas oublier la dernière matière !
if matiere_courante is not None:
    emettre(matiere_courante, somme, nb)
```

**Résultats attendus et obtenus :** Informatique 15.25, Maths 12.50, Physique 11.75.

Avec combiner, les 12 notes sont réduites à 5 couples (somme, nombre) avant le transfert ; `Reduce input groups = 3` (trois matières).

### Exercice 6 — Index inversé

**Clé = le mot, valeur = le nom du fichier** : on inverse la relation fichier → mots. Le mapper obtient le nom du fichier lu grâce à la variable d'environnement `mapreduce_map_input_file`, définie par Hadoop Streaming. Fichiers de test : `doc1.txt` (« le chat dort sur le tapis »), `doc2.txt` (« le chien court dans le jardin »), `doc3.txt` (« le chat court apres le chien »).

`mapper_index.py`

```python
#!/usr/bin/env python3
"""mapper_index.py : émet (mot, nom_du_fichier) pour chaque mot lu.
Le nom du fichier vient de la variable d'environnement
mapreduce_map_input_file, définie par Hadoop Streaming."""
import sys
import os
import re

# chemin complet du fichier traité par cette tâche Map
chemin = os.environ.get("mapreduce_map_input_file", "inconnu")
fichier = os.path.basename(chemin)          # ne garde que doc1.txt

for ligne in sys.stdin:
    ligne = ligne.strip().lower()
    mots = re.findall(r"[a-zàâäéèêëîïôöùûüç]+", ligne)
    for mot in mots:
        print(f"{mot}\t{fichier}")           # clé = mot, valeur = fichier
```

`reducer_index.py`

```python
#!/usr/bin/env python3
"""reducer_index.py : pour chaque mot, liste des fichiers distincts
qui le contiennent. Les lignes arrivent triées par mot (clé)."""
import sys

mot_courant = None
fichiers = set()          # un set évite les doublons (ex. "le" 2 fois dans doc1)

def emettre(mot, fichiers):
    print(f"{mot}\t{', '.join(sorted(fichiers))}")

for ligne in sys.stdin:
    champs = ligne.rstrip("\n").split("\t")
    if len(champs) != 2:
        continue                      # ligne mal formée
    mot, fichier = champs
    if mot == mot_courant:
        fichiers.add(fichier)         # même mot : on ajoute le fichier
    else:
        if mot_courant is not None:
            emettre(mot_courant, fichiers)
        mot_courant = mot
        fichiers = {fichier}

# ne pas oublier le dernier mot !
if mot_courant is not None:
    emettre(mot_courant, fichiers)
```

**Test local** (la variable d'environnement est simulée fichier par fichier) :

```bash
for f in docs/*.txt; do
  mapreduce_map_input_file=$f ./mapper_index.py < $f
done | sort | ./reducer_index.py
```

Le résultat sur HDFS est identique au test local : la variable `mapreduce_map_input_file` est donc bien transmise par Hadoop Streaming. Le mot « le » n'est listé qu'une fois par fichier grâce au `set`.

### 5.5 Plusieurs reducers

Avec `-D mapreduce.job.reduces=3` (placé juste après le nom du jar), le job produit trois fichiers `part-00000`, `part-00001` et `part-00002`. Chaque reducer reçoit une partie des clés (partitionnement par hachage de la clé modulo le nombre de reducers) et **chaque mot n'apparaît que dans un seul fichier**.

> **À compléter** — voir la section « Éléments à compléter ».

---

## Partie 6 — Dépannage et passage à un vrai cluster

### 6.1 Problèmes fréquents (tableau du TP)

| Symptôme | Cause probable | Solution |
|---|---|---|
| `JAVA_HOME is not set` | Variable absente de `hadoop-env.sh` | Ajouter `export JAVA_HOME=...` |
| `Permission denied (publickey)` | SSH sans mot de passe mal configuré | Refaire l'étape 2.3, vérifier `ssh localhost` |
| `pdsh@...: Connection refused` | pdsh utilise rsh au lieu de ssh | `export PDSH_RCMD_TYPE=ssh` dans `~/.bashrc` |
| DataNode absent de `jps` | Reformatage du NameNode (clusterID différent) | Arrêter, vider `~/hadoopdata/datanode`, redémarrer |
| `Could not find or load main class ...MRAppMaster` | `HADOOP_MAPRED_HOME` non transmis | Vérifier les propriétés `*.env` de `mapred-site.xml` |
| `PipeMapRed.waitOutputThreads(): subprocess failed with code 1` | Erreur dans le script Python | Tester avec `cat | mapper | sort | reducer` ; logs sur le port 8088 |
| `Output directory ... already exists` | Répertoire de sortie existant | `hdfs dfs -rm -r <sortie>` |
| `Name node is in safe mode` | NameNode en cours de démarrage | Attendre, ou `hdfs dfsadmin -safemode leave` |

### 6.2 Problèmes rencontrés pendant ce TP

**1. DataNode absent de `jps` (`Incompatible clusterIDs`).** Voir partie 2.7. Cause : plusieurs formatages du NameNode. Solution : arrêter le cluster, vider `~/hadoopdata/datanode`, redémarrer, sans reformater.

**2. Archive Hadoop absente ou incomplète.** Le `mv hadoop-3.3.6 hadoop` a échoué (*No such file or directory*) car l'extraction `tar` avait échoué (*Error is not recoverable*). Solution : retélécharger avec `wget -c`, vérifier la taille (697 Mo), tester l'archive avec `tar -tzf`, puis extraire.

**3. Erreur 404 au téléchargement.** L'adresse avait été mal recopiée : il manquait le tiret dans `hadoop-3.3.6` et le sous-dossier `hadoop-3.3.6/`. L'URL correcte est `https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz`.

**4. Variables d'environnement perdues.** Tapées dans le terminal sans être enregistrées dans `~/.bashrc`, elles disparaissaient à chaque nouvelle session (`hadoop: command not found`). Solution : les ajouter à `~/.bashrc` puis `source ~/.bashrc`.

**5. `poweroff` refusé en session SSH.** Le message *Access denied … interactive authentication has not been enabled* vient de l'absence de `sudo`. Solution : `sudo poweroff`, après avoir arrêté Hadoop avec `stop-yarn.sh` puis `stop-dfs.sh`.

**6. `Output directory ... already exists` (exercice 4).** Lors du premier lancement du job météo, Hadoop a refusé de démarrer : le dossier `output_meteo` existait déjà (`FileAlreadyExistsException`, *Streaming Command Failed!*). Le `cat` qui suivait affichait le résultat de l'ancien run. Solution : `hdfs dfs -rm -r /user/hadoop/tp/output_meteo`, puis relancer.

**7. Échec de résolution DNS (téléchargement du livre).** `wget` vers `www.gutenberg.org` a échoué (*Temporary failure in name resolution*) : le fichier `miserables.txt` faisait **0 octet**. Les jobs lancés dessus ont « réussi » mais sur un fichier vide (`Map input records = 0`) : ils ne prouvent rien et doivent être refaits.

*Diagnostic :* le `ping 8.8.8.8` répond (0 % de perte) : le **réseau fonctionne**. `/etc/resolv.conf` indique `nameserver 127.0.0.53` (résolveur local `systemd-resolved`), qui ne trouve pas de serveur DNS en amont. Le problème vient donc du **DNS** et non du réseau.

**8. Absence de droits `sudo` pour l'utilisateur `hadoop`.** Pour corriger le DNS, la configuration de `systemd-resolved` doit être modifiée avec `sudo`. Les commandes ont répondu *sudo: I'm sorry hadoop. I'm afraid I can't do that* : `hadoop` n'est pas dans le groupe `sudo` (la commande `usermod -aG sudo hadoop` de l'étape 2.2 était facultative). Solution : depuis un compte administrateur, `sudo usermod -aG sudo hadoop`, puis se reconnecter. Alternative sans `sudo` : utiliser un texte de substitution (par exemple les fichiers `LICENSE` de Hadoop) en le signalant.

### 6.3 Vers un cluster multi-nœuds

Pour passer à un cluster réel (1 maître et 2 esclaves) :

1. Installer Java, SSH et Hadoop (même version, même chemin) sur toutes les machines.
2. Déclarer les machines dans `/etc/hosts` (ex. `192.168.1.10 master`, `192.168.1.11 worker1`, `192.168.1.12 worker2`).
3. Copier la clé SSH du maître vers chaque esclave : `ssh-copy-id hadoop@worker1`.
4. Dans `core-site.xml`, remplacer `localhost` par `master` : `hdfs://master:9000`.
5. Dans `yarn-site.xml`, ajouter `yarn.resourcemanager.hostname = master`.
6. Passer `dfs.replication` à 2 ou 3 dans `hdfs-site.xml`.
7. Lister les esclaves dans `$HADOOP_HOME/etc/hadoop/workers` (un nom d'hôte par ligne).
8. Copier la configuration sur les esclaves (`scp -r`), formater le NameNode sur le maître, puis lancer `start-dfs.sh` et `start-yarn.sh` depuis le maître.

**Vérification :** `hdfs dfsadmin -report` doit indiquer autant de « Live datanodes » que d'esclaves, et `http://master:8088/cluster/nodes` doit lister tous les NodeManagers. Les programmes MapReduce et les commandes HDFS ne changent pas : seule la configuration évolue (scalabilité horizontale).

---

## Questions de synthèse

**1. Définition d'un cluster et trois avantages.**
Un cluster est un ensemble d'ordinateurs indépendants (les nœuds), reliés par un réseau rapide et coordonnés par un logiciel, qui fonctionnent comme un seul système aux yeux de l'utilisateur. Trois avantages : (1) le **volume**, car les fichiers sont répartis sur de nombreux disques ; (2) la **puissance de calcul**, car les traitements s'exécutent en parallèle ; (3) la **haute disponibilité**, car si un nœud tombe en panne les autres prennent le relais.

**2. Scalabilité verticale et horizontale.**
La scalabilité verticale (scale-up) rend une machine plus puissante (plus de CPU, de RAM) : plafond matériel vite atteint, coût qui croît très vite, point unique de défaillance. La scalabilité horizontale (scale-out) ajoute des machines au cluster : quasi illimitée, coût linéaire, tolérance aux pannes. **Hadoop exploite la scalabilité horizontale.**

**3. Rôle du NameNode et point sensible.**
Le NameNode gère l'espace de noms (arborescence, droits) et sait sur quels DataNodes se trouve chaque bloc ; il ne stocke aucune donnée utilisateur. Sans lui, personne ne sait où sont les blocs : les fichiers deviennent inaccessibles même si les DataNodes sont intacts. C'est un **point unique de défaillance**, que l'on protège en production par une configuration haute disponibilité.

**4. Le Secondary NameNode est-il une copie de secours ?**
**Non.** Il fusionne périodiquement le journal des modifications (`edits`) avec l'image du système de fichiers (`fsimage`) pour éviter que le journal grossisse indéfiniment. Il ne prend pas le relais si le NameNode tombe en panne.

**5. Localité des données.**
Elle consiste à envoyer le programme (petit) là où se trouvent les données (volumineuses), plutôt que l'inverse. Cela évite de saturer le réseau, essentiel avec des téraoctets de données. Dans l'exercice 4, `Data-local map tasks = 2` montre que les deux tâches Map ont été exécutées sur le nœud qui stockait les données.

**6. Phases d'un job MapReduce.**
Dans l'ordre : Input/Split (Hadoop) → **Map** (développeur) → **Combine**, facultatif (développeur) → Partition (Hadoop) → Shuffle & Sort (Hadoop) → **Reduce** (développeur) → Output (Hadoop). Le développeur écrit donc Map, Combine et Reduce.

**7. Pourquoi le reducer Python peut-il supposer que les lignes d'une même clé sont consécutives ?**
Parce que Hadoop effectue la phase **Shuffle & Sort** : il trie les paires par clé avant de les donner au reducer. C'est ce que simule la commande `sort` dans le test local `cat | mapper | sort | reducer`. Le reducer n'a qu'à détecter un changement de clé pour émettre le résultat de la clé précédente.

**8. Dans quels cas un combiner est-il utilisable ?**
Quand l'opération est **associative et commutative** et que le format de sortie du combiner est identique à celui du mapper. Exemples : la somme (WordCount) et le maximum (exercice 4, vérifié : `Reduce input records` passe de 6 à 4 avec le même résultat). Contre-exemple : la **moyenne** (exercice 5), car la moyenne des moyennes n'est pas la moyenne globale ; il faut alors un combiner dédié qui fait circuler des couples (somme, nombre).

**9. Que se passe-t-il si un nœud tombe en panne pendant une tâche Map ?**
Le nœud cesse d'envoyer ses signaux de vie (heartbeat), la tâche est déclarée échouée et l'ApplicationMaster la **relance automatiquement** sur un autre nœud, en lisant une autre réplique du bloc. Le job continue sans intervention : c'est le principe « la panne est considérée comme normale ».

**10. Deux limites de MapReduce et un outil de l'écosystème qui y répond.**
(1) MapReduce est lourd pour les traitements itératifs, car il écrit sur disque entre chaque job. (2) Sa latence est élevée, ce qui le rend inadapté au temps réel. **Spark** répond à ces deux limites en calculant en mémoire (Hive apporte en plus des requêtes de type SQL).

---

## Conclusion

Le TP a permis d'installer un cluster Hadoop pseudo-distribué (HDFS + YARN), d'observer le découpage d'un fichier en blocs, puis d'écrire et d'exécuter cinq programmes MapReduce en Python avec Hadoop Streaming : comptage de mots, chiffre d'affaires par ville, maximum par année, moyenne par matière et index inversé. Le **choix de la clé** détermine à chaque fois le regroupement des données ; le **combiner** réduit le trafic réseau uniquement quand l'opération est associative et commutative (démontré avec le maximum, contourné avec des couples somme/nombre pour la moyenne).

Les difficultés rencontrées (DataNode absent à cause d'un clusterID incompatible, variables d'environnement non enregistrées, dossier de sortie déjà existant, DNS défaillant) sont autant de cas de dépannage réels, documentés en partie 6.

---

---

## Éléments à compléter avant remise

*(Section de travail à supprimer une fois les manipulations refaites.)*

- **Exercice 2, étape 5 :** WordCount sur un vrai texte, avec et sans combiner (top 20 des mots, comparaison de `Reduce input records`).
- **Section 5.5 :** job avec 3 reducers (trois fichiers `part-` non vides, mots différents dans chacun).

Ces deux manipulations nécessitent un fichier texte **non vide** : le téléchargement de `miserables.txt` ayant échoué (fichier de 0 octet, `Map input records = 0`), les jobs lancés dessus ne sont pas exploitables. Si le DNS reste indisponible, utiliser un texte de substitution (par exemple `cat ~/hadoop/LICENSE-binary ~/hadoop/LICENSE.txt ~/hadoop/NOTICE-binary > ~/tp/miserables.txt`) en le précisant dans le compte rendu.
