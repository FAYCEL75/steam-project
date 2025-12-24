# Databricks notebook source
# MAGIC %md
# MAGIC # 🕹️ Analyse du marché des jeux Steam
# MAGIC
# MAGIC Projet de fin de formation Jedha – Mission d’analyse pour **Ubisoft**
# MAGIC
# MAGIC Objectifs principaux :
# MAGIC
# MAGIC - Comprendre la structure du catalogue Steam (genres, plateformes, âges, langues, prix…)
# MAGIC - Identifier les **facteurs de popularité** (reviews, notes, prix…)
# MAGIC - Analyser l’impact de la **période COVID**
# MAGIC - Dégager des **opportunités marché** pour le lancement d’un nouveau jeu Ubisoft
# MAGIC
# MAGIC ## Plan du notebook
# MAGIC
# MAGIC 1.  Setup & chargement des données  
# MAGIC 2.  Diagnostic du schéma brut (sans champs cachés)  
# MAGIC 3.  Construction de la table principale `games_df`  
# MAGIC 4.  Nettoyage & Feature Engineering (types, dates, reviews, COVID)  
# MAGIC 5.  Analyse macro du marché (années, publishers, prix, âges…)  
# MAGIC 6.  Analyse par genres (fréquence, satisfaction, plateformes)  
# MAGIC 7.  Analyse par plateformes (Windows / Mac / Linux)  
# MAGIC 8.  Synthèse & recommandations business pour Ubisoft

# COMMAND ----------

# 1. Setup & chargement des données depuis S3 (PySpark / Databricks)

from pyspark.sql import functions as F
from pyspark.sql import types as T
import json

# Chemin fourni par Jedha
steam_path = "s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json"

# Lecture du JSON semi-structuré
df = spark.read.json(steam_path)

print("✅ Données brutes chargées depuis :", steam_path)
print("✅ Nombre total de lignes brutes :", df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Diagnostic du schéma brut
# MAGIC
# MAGIC Avant de construire notre table métier `games_df`, on veut vérifier :
# MAGIC
# MAGIC - la structure exacte de `data` (struct imbriquée)
# MAGIC - la présence des champs clés : `appid`, `name`, `genre`, `publisher`, `price`, `initialprice`, `discount`, `platforms`, `release_date`, `positive`, `negative`, `languages`, `owners`, `ccu`, etc.
# MAGIC
# MAGIC On affiche le schéma en JSON pour **ne cacher aucune colonne**, même si elle est imbriquée.

# COMMAND ----------

# 2.1 Schéma complet en JSON

print("🔎 SCHÉMA COMPLET (JSON compact)")
print(df.schema.json())

print("\n🔎 SCHÉMA COMPLET (JSON indenté)")
print(json.dumps(df.schema.jsonValue(), indent=2))

print("\n Schéma brut affiché.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Construction de la table principale `games_df`
# MAGIC
# MAGIC Objectif : aplatir la structure `data.*` dans une table **plate** et exploitable `games_df`.
# MAGIC
# MAGIC On va :
# MAGIC
# MAGIC - garder un identifiant interne (`id`) + l’`app_id` Steam
# MAGIC - extraire les principaux attributs d’un jeu :
# MAGIC   - métadonnées : `name`, `genre`, `publisher`, `developer`, `type`
# MAGIC   - prix : `price`, `initialprice`, `discount`
# MAGIC   - succès : `positive`, `negative`, `owners`, `ccu`
# MAGIC   - classification : `required_age`
# MAGIC   - accessibilité : `languages`
# MAGIC   - plateformes : `platforms.linux`, `platforms.mac`, `platforms.windows`
# MAGIC   - temporalité : `release_date`
# MAGIC
# MAGIC Tous les champs seront d’abord créés en version brute (`*_raw`) pour garder la trace du format original.

# COMMAND ----------

# 3.1 Construction de la table principale des jeux à partir de df.data

def build_games_df(df):
    return (
        df
        # Identifiants
        .withColumn("id", F.col("id"))
        .withColumn("app_id", F.col("data.appid"))
        .withColumn("name", F.col("data.name"))

        # Métadonnées
        .withColumn("genre_raw", F.col("data.genre"))
        .withColumn("publisher", F.col("data.publisher"))
        .withColumn("developer", F.col("data.developer"))
        .withColumn("type", F.col("data.type"))

        # Prix & discount (brut)
        .withColumn("price_raw", F.col("data.price"))
        .withColumn("initialprice_raw", F.col("data.initialprice"))
        .withColumn("discount_raw", F.col("data.discount"))

        # Reviews & âge (brut)
        .withColumn("required_age_raw", F.col("data.required_age"))
        .withColumn("positive_raw", F.col("data.positive"))
        .withColumn("negative_raw", F.col("data.negative"))

        # Langues & owners
        .withColumn("languages_raw", F.col("data.languages"))
        .withColumn("owners_raw", F.col("data.owners"))
        .withColumn("ccu", F.col("data.ccu"))

        # Plateformes
        .withColumn("platform_linux", F.col("data.platforms.linux"))
        .withColumn("platform_mac", F.col("data.platforms.mac"))
        .withColumn("platform_windows", F.col("data.platforms.windows"))

        # Date de sortie (brute)
        .withColumn("release_date_raw", F.col("data.release_date"))
    )

games_df = build_games_df(df)

print("Table de base `games_df` construite")
print("Nombre de jeux distincts :", games_df.select("id").distinct().count())

# Aperçu contrôlé pour éviter l’affichage automatique tronqué
games_df.select(
    "id", "app_id", "name", "genre_raw", "price_raw",
    "publisher", "platform_windows", "platform_mac", "platform_linux",
    "release_date_raw"
).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Vérification des colonnes de `games_df`
# MAGIC
# MAGIC On liste les colonnes et leurs types pour vérifier que la **projection depuis `data.*`** est correcte.

# COMMAND ----------

print("Colonnes de `games_df` :\n")
for col_name, col_type in games_df.dtypes:
    print(f"• {col_name:<25} {col_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Nettoyage & Feature Engineering
# MAGIC
# MAGIC Dans cette section, on transforme les champs bruts en variables analytiques propres :
# MAGIC
# MAGIC - Conversion des champs numériques (`price`, `initialprice`, `discount`, `positive`, `negative`, `required_age`, `ccu`)
# MAGIC - Nettoyage et parsing **robuste** de la date de sortie avec ton code (multi-formats, padding, etc.)
# MAGIC - Création des variables temporelles : `release_year`, `release_month`, `covid_period`
# MAGIC - Création des variables de popularité : `total_reviews`, `positive_ratio`
# MAGIC
# MAGIC L’objectif : obtenir un `games_df` directement exploitable pour toutes les analyses suivantes.

# COMMAND ----------

# 4.1 Normalisation des champs numériques (prix, reviews, âge, ccu)

games_df = (
    games_df
    # Prix : conversion en double
    .withColumn("price", F.col("price_raw").cast(T.DoubleType()))
    .withColumn("initialprice", F.col("initialprice_raw").cast(T.DoubleType()))
    .withColumn("discount", F.col("discount_raw").cast(T.DoubleType()))
    
    # Reviews : conversion en long
    .withColumn("positive", F.col("positive_raw").cast(T.LongType()))
    .withColumn("negative", F.col("negative_raw").cast(T.LongType()))
    
    # Age : garder une version string + une version numérique quand possible
    .withColumn("required_age_str", F.col("required_age_raw").cast(T.StringType()))
    .withColumn(
        "required_age",
        F.regexp_extract(F.col("required_age_str"), r"(\d+)", 1).cast(T.IntegerType())
    )
    
    # Concurrents connectés simultanément (ccu)
    .withColumn("ccu", F.col("ccu").cast(T.LongType()))
)

print("✅ Normalisation numérique effectuée")

games_df.select(
    "name", "price_raw", "price", "initialprice_raw", "initialprice",
    "discount_raw", "discount", "required_age_raw", "required_age",
    "positive_raw", "positive", "negative_raw", "negative"
).show(5)

# COMMAND ----------

# Ajout de colonnes dédiées : price_eur, initialprice_eur

games_df = (
    games_df
    .withColumn("price_eur", F.col("price") / 100)
    .withColumn("initialprice_eur", F.col("initialprice") / 100)
)

print("Conversion effectuée : price_eur & initialprice_eur ajoutées.")
games_df.select("name", "price", "price_eur", "initialprice", "initialprice_eur").show(5, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Nettoyage & parsing robuste de la date de sortie
# MAGIC
# MAGIC Ici, on applique **exactement ton pipeline de nettoyage**, en 5 étapes :
# MAGIC
# MAGIC 1. Suppression des virgules (ex : `"Oct 21, 2008"` → `"Oct 21 2008"`)  
# MAGIC 2. Remplacement des `/` par des `-` (ex : `2000/11/1` → `2000-11-1`)  
# MAGIC 3. Padding des jours/mois à un chiffre (ex : `2000-11-1` → `2000-11-01`)  
# MAGIC 4. Parsing multi-formats via `F.coalesce` pour gérer plusieurs formats possibles  
# MAGIC 5. Sécurité : si imparsable, on laisse `NULL` (Spark ne lève pas d’exception, mais on garde ta logique)
# MAGIC
# MAGIC On obtient ainsi une colonne propre : `release_date_parsed`.
# MAGIC

# COMMAND ----------

# 4.2.1 Nettoyage des virgules (ex : "Oct 21, 2008" -> "Oct 21 2008")
games_df = games_df.withColumn(
    "release_date_clean",
    F.regexp_replace(F.col("release_date_raw"), ",", "")
)

# 4.2.2 Uniformisation des séparateurs "/" -> "-"
games_df = games_df.withColumn(
    "release_date_clean",
    F.regexp_replace("release_date_clean", "/", "-")
)

# 4.2.3 Padding des jours/mois à un chiffre (ex : 2000-11-1 -> 2000-11-01)
games_df = games_df.withColumn(
    "release_date_clean",
    F.regexp_replace("release_date_clean", r"-(\d)(?!\d)", r"-0$1")
)

# 4.2.4 Parsing multi-formats sécurisé
games_df = games_df.withColumn(
    "release_date_parsed",
    F.coalesce(
        F.to_date("release_date_clean", "MMM d yyyy"),
        F.to_date("release_date_clean", "MMM dd yyyy"),
        F.to_date("release_date_clean", "yyyy-MM-dd"),
        F.to_date("release_date_clean", "dd MMM yyyy"),
        F.to_date("release_date_clean", "d MMM yyyy")
    )
)

# 4.2.5 Sécurité finale : laisser NULL si imparsable, sans lever d'exception
games_df = games_df.withColumn(
    "release_date_parsed",
    F.when(F.col("release_date_parsed").isNull(), None)
     .otherwise(F.col("release_date_parsed"))
)

print("✅ Nettoyage + parsing des dates terminé")

display(
    games_df.select(
        "release_date_raw", "release_date_clean", "release_date_parsed"
    ).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Variables temporelles dérivées & période COVID
# MAGIC
# MAGIC À partir de `release_date_parsed`, on dérive :
# MAGIC
# MAGIC - `release_year` : année de sortie
# MAGIC - `release_month` : mois de sortie
# MAGIC - `covid_period` :
# MAGIC   - `pre_covid` : avant 2019
# MAGIC   - `covid` : entre 2019 et 2021 inclus
# MAGIC   - `post_covid` : après 2021
# MAGIC   - `unknown` : dates manquantes ou imparsables

# COMMAND ----------

# 4.3.1 Création des variables temporelles

games_df = games_df.withColumn("release_year", F.year("release_date_parsed"))
games_df = games_df.withColumn("release_month", F.month("release_date_parsed"))

COVID_START_YEAR = 2019
COVID_END_YEAR = 2021

games_df = games_df.withColumn(
    "covid_period",
    F.when(F.col("release_year").isNull(), F.lit("unknown"))
     .when(F.col("release_year") < COVID_START_YEAR, F.lit("pre_covid"))
     .when(
         (F.col("release_year") >= COVID_START_YEAR) &
         (F.col("release_year") <= COVID_END_YEAR),
         F.lit("covid")
     )
     .otherwise(F.lit("post_covid"))
)

print("✅ Variables temporelles créées : release_year, release_month, covid_period")

games_df.select(
    "name", "release_date_raw", "release_date_parsed",
    "release_year", "release_month", "covid_period"
).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Variables de popularité : `total_reviews` & `positive_ratio`
# MAGIC
# MAGIC On reconstruit proprement les indicateurs de popularité :
# MAGIC
# MAGIC - `total_reviews` = `positive + negative`
# MAGIC - `positive_ratio` = `positive / total_reviews` (quand `total_reviews > 0`)
# MAGIC
# MAGIC Ces deux variables seront utilisées pour :
# MAGIC
# MAGIC - identifier les jeux “stars”
# MAGIC - comparer l’attractivité des genres
# MAGIC - faire des filtres par volume (éviter les jeux avec 3 reviews et un 100 % trompeur)

# COMMAND ----------

# 4.4.1 Reconstruction sûre des colonnes de reviews

games_df = (
    games_df
    .withColumn("positive", F.col("positive").cast(T.LongType()))
    .withColumn("negative", F.col("negative").cast(T.LongType()))
    .withColumn(
        "total_reviews",
        (F.col("positive") + F.col("negative")).cast(T.LongType())
    )
    .withColumn(
        "positive_ratio",
        F.when(F.col("total_reviews") > 0,
               F.col("positive").cast(T.DoubleType()) / F.col("total_reviews"))
         .otherwise(None)
    )
)

print("Colonnes reviews reconstruites : positive, negative, total_reviews, positive_ratio")

games_df.select(
    "name", "positive", "negative", "total_reviews", "positive_ratio"
).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Résumé du `games_df` final (base d’analyse)
# MAGIC
# MAGIC On vérifie maintenant que `games_df` contient :
# MAGIC
# MAGIC - les colonnes métier attendues,
# MAGIC - les variables dérivées prêtes pour l’EDA :
# MAGIC
# MAGIC   - `release_year`, `release_month`, `covid_period`
# MAGIC   - `total_reviews`, `positive_ratio`
# MAGIC   - `platform_windows`, `platform_mac`, `platform_linux`
# MAGIC   - `price`, `discount`, `required_age`

# COMMAND ----------

print("Schéma final de `games_df` (colonnes principales) :\n")

for col_name in [
    "id", "app_id", "name", "genre_raw", "publisher", "developer", "type",
    "price", "initialprice", "discount",
    "required_age_str", "required_age",
    "positive", "negative", "total_reviews", "positive_ratio",
    "languages_raw", "owners_raw", "ccu",
    "platform_windows", "platform_mac", "platform_linux",
    "release_date_raw", "release_date_parsed", "release_year", "release_month", "covid_period"
]:
    if col_name in games_df.columns:
        print(f"• {col_name}")

print("\n`games_df` est prêt pour l’analyse.")
games_df.select(
    "name", "genre_raw", "price", "discount",
    "release_year", "covid_period",
    "total_reviews", "positive_ratio",
    "platform_windows", "platform_mac", "platform_linux"
).show(5, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Analyse Macro du Marché Steam
# MAGIC
# MAGIC Dans cette section, nous analysons :
# MAGIC
# MAGIC - La répartition des jeux par plateforme  
# MAGIC - Le volume de sorties par année  
# MAGIC - L’impact des périodes COVID  
# MAGIC - Les publishers les plus prolifiques  
# MAGIC - La distribution des prix  
# MAGIC - Les tranches d’âge (classification PEGI “like”)  
# MAGIC - Les langues les plus représentées
# MAGIC
# MAGIC Ces analyses servent à comprendre les tendances globales du marché pour guider Ubisoft dans le lancement d’un nouveau jeu.

# COMMAND ----------

# 5.1 — Répartition des jeux par plateforme
platform_counts = (
    games_df.select(
        F.col("platform_windows").alias("Windows"),
        F.col("platform_mac").alias("Mac"),
        F.col("platform_linux").alias("Linux")
    )
)

windows_count = platform_counts.filter("Windows = true").count()
mac_count = platform_counts.filter("Mac = true").count()
linux_count = platform_counts.filter("Linux = true").count()

print("🎮 Répartition des jeux par plateforme :")
print(f"• Windows : {windows_count}")
print(f"• macOS   : {mac_count}")
print(f"• Linux   : {linux_count}")

# Vue Databricks (pour graphiques pie chart)
display(platform_counts)

# COMMAND ----------

# 5.2 — Nombre de sorties par année
release_per_year = (
    games_df.groupBy("release_year")
            .count()
            .orderBy("release_year")
)

display(release_per_year)

release_per_year.show(10)

# COMMAND ----------

# 5.3 — Impact de la période COVID
covid_counts = (
    games_df.groupBy("covid_period")
            .count()
            .orderBy("covid_period")
)

print("Sorties par période COVID :")
display(covid_counts)

# COMMAND ----------

# 5.4 — Publishers les plus prolifiques
publisher_counts = (
    games_df.groupBy("publisher")
            .count()
            .orderBy(F.desc("count"))
            .limit(50)
)

print("Top 50 publishers Steam par nombre de sorties :")
display(publisher_counts)

publisher_counts.show(5)

# COMMAND ----------

# 5.5 — Distribution des prix
price_dist = (
    games_df
    .select("price")
    .filter("price IS NOT NULL AND price > 0")
)

display(price_dist)

price_dist.summary().show()

# COMMAND ----------

# 5.6 — Jeux en promotion
discounted_games = games_df.filter("discount > 0")
discounted_percentage = discounted_games.count() / games_df.count() * 100

print(f"Jeux actuellement en promotion : {discounted_games.count()} "
      f"({discounted_percentage:.2f} %)")

# COMMAND ----------

# 5.7 — Classification par âge (PEGI-like)
age_dist = (
    games_df
    .select("required_age")
)

print("Répartition des jeux par âge :")
display(age_dist)

age_dist.groupBy("required_age").count().orderBy("required_age").show()

# COMMAND ----------

# 5.8 — Nombre de langues supportées
# Création array de langues si pas encore fait
games_df = games_df.withColumn(
    "languages_array",
    F.split(F.col("languages_raw"), ",")
)

games_df = games_df.withColumn(
    "num_languages",
    F.size("languages_array")
)

languages_dist = games_df.select("num_languages")

print("Diversité linguistique des jeux Steam :")
display(languages_dist)

languages_dist.groupBy("num_languages").count().orderBy("num_languages").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Analyse par genres
# MAGIC
# MAGIC Les genres sont **centrales** pour Ubisoft :
# MAGIC
# MAGIC - Ils décrivent le **positionnement gameplay** du jeu (Action, Adventure, RPG, Strategy…)
# MAGIC - Ils influencent :
# MAGIC   - le **public cible**,
# MAGIC   - la **plateforme privilégiée**,
# MAGIC   - la **probabilité de succès** (reviews, bouche-à-oreille),
# MAGIC   - la **concurrence**.
# MAGIC
# MAGIC Dans cette section, on va :
# MAGIC
# MAGIC 1. Construire une table **explosée par genre** (`genres_exploded_df`)  
# MAGIC 2. Analyser la **fréquence des genres**  
# MAGIC 3. Mesurer la **satisfaction moyenne par genre** (`positive_rate`)  
# MAGIC 4. Identifier les **genres “haut potentiel”** (volume + satisfaction)  
# MAGIC 5. Étudier le lien **genres ↔ plateformes** (Windows / macOS / Linux)
# MAGIC

# COMMAND ----------

# 6.1 Construction d'une table "un jeu = un genre" (explosion des genres)

# On découpe "genre_raw" en liste de genres, en gérant :
# - les séparateurs par virgule
# - les espaces éventuels après les virgules
games_df = games_df.withColumn(
    "genre_array",
    F.split(F.col("genre_raw"), r",\s*")
)

# Explosion : chaque ligne devient (jeu, genre_unique)
genres_exploded_df = (
    games_df
    .withColumn("genre", F.explode("genre_array"))
    .filter(F.col("genre").isNotNull() & (F.col("genre") != ""))
)

print("Table genres_exploded_df construite")
print("Nombre de lignes (jeu × genre) :", genres_exploded_df.count())
print("Nombre de genres uniques :", genres_exploded_df.select("genre").distinct().count())

genres_exploded_df.select(
    "name", "genre_raw", "genre", "price", "total_reviews", "positive_ratio"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Genres les plus représentés
# MAGIC
# MAGIC Objectifs :
# MAGIC
# MAGIC - Repérer les **genres dominants** sur Steam  
# MAGIC - Identifier les **genres de niche**  
# MAGIC - Positionner Ubisoft par rapport aux tendances (AAA souvent Action / Adventure / RPG)
# MAGIC
# MAGIC On calcule le **nombre de jeux distincts** par genre.

# COMMAND ----------

# 6.2 Comptage des jeux par genre

genre_counts_df = (
    genres_exploded_df
    .groupBy("genre")
    .agg(
        F.countDistinct("app_id").alias("nb_games")
    )
    .orderBy(F.desc("nb_games"))
)

print("Top 20 genres les plus représentés :")
genre_counts_df.show(20, truncate=False)

# Vue Databricks pour visualisation (bar chart)
display(genre_counts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Satisfaction par genre (ratio de reviews positives)
# MAGIC
# MAGIC On cherche à répondre à :
# MAGIC
# MAGIC - Quels genres ont les **meilleures évaluations moyennes** ?
# MAGIC - Quels genres combinent **volume** (nb de jeux, nb de reviews) et **satisfaction** ?
# MAGIC
# MAGIC On agrège par genre :
# MAGIC
# MAGIC - `sum_positive` : nombre total de reviews positives
# MAGIC - `sum_negative` : nombre total de reviews négatives
# MAGIC - `sum_total_reviews` : total des reviews
# MAGIC - `positive_rate` = `sum_positive / sum_total_reviews`

# COMMAND ----------

# 6.3 Agrégation des reviews par genre

genre_reviews_df = (
    genres_exploded_df
    .groupBy("genre")
    .agg(
        F.countDistinct("app_id").alias("nb_games"),
        F.sum("positive").alias("sum_positive"),
        F.sum("negative").alias("sum_negative"),
        F.sum("total_reviews").alias("sum_total_reviews")
    )
    .withColumn(
        "positive_rate",
        F.when(F.col("sum_total_reviews") > 0,
               F.col("sum_positive") / F.col("sum_total_reviews"))
         .otherwise(None)
    )
)

print("Aperçu brut des stats par genre :")
genre_reviews_df.select(
    "genre", "nb_games", "sum_total_reviews", "positive_rate"
).orderBy(F.desc("sum_total_reviews")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Genres “haut potentiel” (volume + satisfaction)
# MAGIC
# MAGIC On définit un **seuil minimum de reviews** pour qu’un genre soit significatif.
# MAGIC
# MAGIC Exemple :
# MAGIC
# MAGIC - `MIN_REVIEWS_GENRE = 10 000` total (somme sur tous les jeux du genre)
# MAGIC
# MAGIC Critères pour un genre “haut potentiel” :
# MAGIC
# MAGIC - `sum_total_reviews >= MIN_REVIEWS_GENRE`
# MAGIC - `positive_rate >= 0.85` (85 % de reviews positives ou plus)
# MAGIC - un nombre suffisant de jeux (`nb_games`) pour être un genre durable
# MAGIC
# MAGIC Cela permet de recommander à Ubisoft :
# MAGIC
# MAGIC - des genres à **fort engagement**
# MAGIC - mais pas des genres “micro-niche”.

# COMMAND ----------

# 6.4 Sélection des genres "haut potentiel"

MIN_REVIEWS_GENRE = 10000

high_potential_genres_df = (
    genre_reviews_df
    .filter(F.col("sum_total_reviews") >= MIN_REVIEWS_GENRE)
    .filter(F.col("positive_rate") >= 0.85)
    .orderBy(F.desc("positive_rate"))
)

print(f"Genres 'haut potentiel' (≥ {MIN_REVIEWS_GENRE} reviews & ≥ 85% positives) :")
high_potential_genres_df.select(
    "genre", "nb_games", "sum_total_reviews", "positive_rate"
).show(20, truncate=False)

display(high_potential_genres_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 “Blockbusters” par genre : popularité × prix
# MAGIC
# MAGIC On cherche un proxy simple de “potentiel business” par genre :
# MAGIC
# MAGIC - Un jeu est considéré comme **“blockbuster-like”** s’il cumule :
# MAGIC   - beaucoup de reviews (`total_reviews` élevé)
# MAGIC   - un prix non nul (jeu payant)
# MAGIC   - un bon `positive_ratio`
# MAGIC
# MAGIC On peut par exemple :
# MAGIC
# MAGIC - filtrer les jeux avec `total_reviews > 50 000` et `positive_ratio > 0.9`
# MAGIC - regarder la répartition de ces jeux par genre

# COMMAND ----------

# 6.5 Jeux "blockbusters" et genres associés

blockbusters_df = (
    genres_exploded_df
    .filter(F.col("total_reviews") > 50000)
    .filter(F.col("positive_ratio") > 0.9)
    .filter(F.col("price") > 0)
)

print("Exemples de jeux 'blockbusters' (reviews >> 50k & >90% positives) :")
blockbusters_df.select(
    "name", "genre", "price", "total_reviews", "positive_ratio"
).distinct().show(20, truncate=False)

blockbuster_genre_counts = (
    blockbusters_df
    .groupBy("genre")
    .agg(
        F.countDistinct("app_id").alias("nb_blockbusters")
    )
    .orderBy(F.desc("nb_blockbusters"))
)

print("Genres les plus présents parmi les 'blockbusters' :")
blockbuster_genre_counts.show(20)

display(blockbuster_genre_counts)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.6 Genres × plateformes
# MAGIC
# MAGIC Question clé pour Ubisoft :
# MAGIC
# MAGIC > “Si on choisit un genre donné, doit-on viser Windows seul, ou aussi macOS / Linux ?”
# MAGIC
# MAGIC On calcule, pour chaque genre :
# MAGIC
# MAGIC - le nombre de jeux Windows
# MAGIC - le nombre de jeux Mac
# MAGIC - le nombre de jeux Linux
# MAGIC
# MAGIC Cela permet d’identifier :
# MAGIC
# MAGIC - les genres **historiquement PC “hardcore”** (ex : Strategy, Simulation, RPG → forte présence Linux)  
# MAGIC - les genres plus “casual / multi-plateformes” (Action, Casual, Indie…)

# COMMAND ----------

# 6.6 Croisement genres × plateformes

genre_platform_df = (
    genres_exploded_df
    .groupBy("genre")
    .agg(
        F.countDistinct("app_id").alias("nb_games"),
        F.sum(F.when(F.col("platform_windows") == True, 1).otherwise(0)).alias("nb_windows"),
        F.sum(F.when(F.col("platform_mac") == True, 1).otherwise(0)).alias("nb_mac"),
        F.sum(F.when(F.col("platform_linux") == True, 1).otherwise(0)).alias("nb_linux")
    )
    .orderBy(F.desc("nb_games"))
)

print("Genres × plateformes (top 30) :")
genre_platform_df.select(
    "genre", "nb_games", "nb_windows", "nb_mac", "nb_linux"
).show(30, truncate=False)

display(genre_platform_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.7 Synthèse – Genres pour Ubisoft
# MAGIC
# MAGIC À partir de cette analyse, Ubisoft peut :
# MAGIC
# MAGIC - **Cibler en priorité** :
# MAGIC   - des genres **fortement représentés** (pour toucher un marché large)
# MAGIC   - ET **bien notés** (positive_rate élevé)
# MAGIC   - ex. : *Action, Adventure, RPG, Strategy, Indie* (à valider avec les chiffres exacts)
# MAGIC
# MAGIC - **Éviter** :
# MAGIC   - les genres sursaturés avec une satisfaction moyenne faible
# MAGIC   - les genres ultra-niche sans base marché suffisante
# MAGIC
# MAGIC - **Penser multi-genres** :
# MAGIC   - de nombreux hits combinent plusieurs genres :  
# MAGIC     *Action + Adventure*, *RPG + Strategy*, *Indie + Puzzle*, etc.
# MAGIC   - Ubisoft peut se positionner sur un **mix de genres** plutôt qu’un seul “pur” genre.

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Analyse détaillée des plateformes
# MAGIC
# MAGIC Objectif : compléter la vision “macro” en regardant **plus finement** le rôle de chaque plateforme :
# MAGIC
# MAGIC - Part de marché de Windows / macOS / Linux
# MAGIC - Part des jeux **exclusifs** vs **multi-plateformes**
# MAGIC - Qualité moyenne (reviews) par plateforme
# MAGIC - Pricing moyen par plateforme
# MAGIC
# MAGIC Ces éléments sont cruciaux pour Ubisoft pour décider :
# MAGIC - s’il faut cibler uniquement Windows
# MAGIC - ou envisager des ports macOS / Linux.
# MAGIC

# COMMAND ----------

# 7.1 Rappel des colonnes plateformes dans games_df

games_df.select(
    "name", "platform_windows", "platform_mac", "platform_linux"
).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Répartition globale par plateforme
# MAGIC
# MAGIC On calcule :
# MAGIC
# MAGIC - le nombre de jeux disponibles sur chaque OS
# MAGIC - la part que cela représente dans le catalogue total.

# COMMAND ----------

total_games = games_df.count()

platform_agg_df = (
    games_df
    .agg(
        F.sum(F.when(F.col("platform_windows") == True, 1).otherwise(0)).alias("windows_count"),
        F.sum(F.when(F.col("platform_mac") == True, 1).otherwise(0)).alias("mac_count"),
        F.sum(F.when(F.col("platform_linux") == True, 1).otherwise(0)).alias("linux_count")
    )
)

platform_counts = platform_agg_df.collect()[0]
windows_count = platform_counts["windows_count"]
mac_count = platform_counts["mac_count"]
linux_count = platform_counts["linux_count"]

print("🎮 Répartition des jeux par plateforme :")
print(f"• Windows : {windows_count} ({windows_count / total_games * 100:.2f} %)")
print(f"• macOS   : {mac_count} ({mac_count / total_games * 100:.2f} %)")
print(f"• Linux   : {linux_count} ({linux_count / total_games * 100:.2f} %)")

platform_share_df = spark.createDataFrame(
    [
        ("Windows", int(windows_count), float(windows_count / total_games * 100)),
        ("macOS",   int(mac_count),    float(mac_count / total_games * 100)),
        ("Linux",   int(linux_count),  float(linux_count / total_games * 100)),
    ],
    ["platform", "nb_games", "share_percent"]
)

display(platform_share_df)
platform_share_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC - Windows est **quasi universel** sur Steam
# MAGIC - macOS et Linux restent **très minoritaires**, mais non négligeables
# MAGIC - Ubisoft doit décider si le coût de portage vers macOS / Linux est justifié par ces parts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Jeux exclusifs vs multi-plateformes
# MAGIC
# MAGIC On distingue :
# MAGIC
# MAGIC - `windows_only`   : uniquement Windows
# MAGIC - `windows_mac`    : Windows + macOS
# MAGIC - `windows_linux`  : Windows + Linux
# MAGIC - `tri_platform`   : Windows + macOS + Linux
# MAGIC
# MAGIC Cela permet de voir si les jeux tendent à être **multi-plateformes** ou non.

# COMMAND ----------

games_df = (
    games_df
    .withColumn(
        "is_windows_only",
        (F.col("platform_windows") == True) &
        (F.col("platform_mac") == False) &
        (F.col("platform_linux") == False)
    )
    .withColumn(
        "is_tri_platform",
        (F.col("platform_windows") == True) &
        (F.col("platform_mac") == True) &
        (F.col("platform_linux") == True)
    )
    .withColumn(
        "is_windows_mac",
        (F.col("platform_windows") == True) &
        (F.col("platform_mac") == True) &
        (F.col("platform_linux") == False)
    )
    .withColumn(
        "is_windows_linux",
        (F.col("platform_windows") == True) &
        (F.col("platform_mac") == False) &
        (F.col("platform_linux") == True)
    )
)

platform_profile_df = (
    games_df
    .agg(
        F.sum(F.when(F.col("is_windows_only"), 1).otherwise(0)).alias("windows_only"),
        F.sum(F.when(F.col("is_tri_platform"), 1).otherwise(0)).alias("tri_platform"),
        F.sum(F.when(F.col("is_windows_mac"), 1).otherwise(0)).alias("windows_mac"),
        F.sum(F.when(F.col("is_windows_linux"), 1).otherwise(0)).alias("windows_linux")
    )
)

display(platform_profile_df)
platform_profile_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC - Une grande partie des jeux restent **Windows-only**, souvent pour des raisons de coût.
# MAGIC - Le véritable “premium segment” technique correspond aux jeux **tri-plateformes**.
# MAGIC - C’est ce segment que Ubisoft vise en général avec ses AAA (qualité d’optimisation élevée).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4 Qualité & popularité moyenne par plateforme
# MAGIC
# MAGIC On veut savoir :
# MAGIC
# MAGIC - Les jeux Windows-only sont-ils **mieux ou moins bien notés** que les tri-plateformes ?
# MAGIC - Les jeux multiplateformes ont-ils tendance à avoir **plus de reviews** (donc plus de visibilité) ?

# COMMAND ----------

# On crée une colonne catégorie de plateforme lisible

games_df = games_df.withColumn(
    "platform_profile",
    F.when(F.col("is_tri_platform"), F.lit("Windows + macOS + Linux"))
     .when(F.col("is_windows_mac"), F.lit("Windows + macOS"))
     .when(F.col("is_windows_linux"), F.lit("Windows + Linux"))
     .when(F.col("is_windows_only"), F.lit("Windows only"))
     .otherwise(F.lit("Others"))
)

platform_quality_df = (
    games_df
    .groupBy("platform_profile")
    .agg(
        F.countDistinct("app_id").alias("nb_games"),
        F.avg("positive_ratio").alias("avg_positive_ratio"),
        F.avg("total_reviews").alias("avg_total_reviews"),
        F.avg("price").alias("avg_price")
    )
    .orderBy(F.desc("nb_games"))
)

display(platform_quality_df)
platform_quality_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Points à commenter** :
# MAGIC
# MAGIC - Les jeux **tri-plateformes** ont en général :
# MAGIC   - un **volume de reviews plus élevé** (plus d’audience),
# MAGIC   - un **niveau de qualité moyen élevé** (positive_ratio).
# MAGIC - Les jeux **Windows-only** peuvent être plus “expérimentaux” / “indie”, avec plus de variance.
# MAGIC
# MAGIC Ubisoft, positionné sur du AAA, est naturellement attendu dans la catégorie
# MAGIC **“Windows + macOS + Linux”**, ou à minima **“Windows + macOS”**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.5 Top jeux par plateforme (exemples concrets à citer)
# MAGIC
# MAGIC On extrait quelques jeux emblématiques par profil de plateforme

# COMMAND ----------

# Windows only – quelques exemples
top_windows_only = (
    games_df
    .filter("is_windows_only = true")
    .orderBy(F.desc("total_reviews"))
    .select("name", "price", "total_reviews", "positive_ratio")
    .limit(10)
)

print("Exemples de jeux populaires 'Windows only' :")
top_windows_only.show(10, truncate=False)

# Tri-plateformes – quelques exemples
top_tri_platform = (
    games_df
    .filter("is_tri_platform = true")
    .orderBy(F.desc("total_reviews"))
    .select("name", "price", "total_reviews", "positive_ratio")
    .limit(10)
)

print("Exemples de jeux populaires 'Windows + macOS + Linux' :")
top_tri_platform.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.6 Synthèse – Plateformes & stratégie Ubisoft
# MAGIC
# MAGIC
# MAGIC - **Windows est incontournable** : quasi tout le catalogue Steam l’utilise.
# MAGIC - macOS et Linux représentent des parts **plus petites mais non négligeables**, surtout sur les genres “hardcore” (Strategy, Simulation, RPG, Indie).
# MAGIC - Les jeux **multi-plateformes** (notamment tri-plateformes) sont souvent :
# MAGIC   - plus visibles (plus de reviews),
# MAGIC   - mieux optimisés,
# MAGIC   - perçus comme plus “premium”.
# MAGIC
# MAGIC **Recommandation pour Ubisoft :**
# MAGIC
# MAGIC > Viser en priorité **Windows + macOS**, avec un port Linux si le jeu cible un public “PC enthusiast” (Strategy / Simulation / RPG), cohérent avec l’ADN Steam.

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. Analyse des Prix & Promotions
# MAGIC
# MAGIC Dans cette section, nous analysons les prix réels des jeux (en euros)  
# MAGIC à partir des colonnes :
# MAGIC
# MAGIC - `price_eur`
# MAGIC - `initialprice_eur`
# MAGIC - `discount`
# MAGIC
# MAGIC Objectifs :
# MAGIC
# MAGIC 1. Distribution des prix (EUR)  
# MAGIC 2. Aperçu des promotions  
# MAGIC 3. Variations de prix selon les genres  
# MAGIC 4. Lien prix ↔ succès (reviews)  
# MAGIC 5. Effets COVID sur les prix

# COMMAND ----------

# Vérification rapide
games_df.select("name", "price_eur", "initialprice_eur", "discount").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.1 Distribution réelle des prix (en euros)
# MAGIC
# MAGIC Caractéristiques principales observées :
# MAGIC
# MAGIC - Prix minimum : **0,28 €**
# MAGIC - Médiane : **5,99 €**
# MAGIC - Moyenne : **8,99 €**
# MAGIC - 75% : **10,00 €**
# MAGIC - Maximum : **999,00 €**
# MAGIC
# MAGIC Ainsi, une large majorité des jeux vendus sur Steam se positionnent entre :
# MAGIC **0,99 € et 9,99 €**.
# MAGIC
# MAGIC Les prix extrêmes (> 200 €) correspondent à :
# MAGIC - bundles
# MAGIC - logiciels (vidéo, 3D…)
# MAGIC - packs premium multi-DLC

# COMMAND ----------

# Statistiques en euros
price_stats = games_df.select("price_eur").summary()
display(price_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Le marché Steam est dominé par les **petits jeux low-cost**.
# MAGIC - Les prix “mass market” se situent entre **5 € et 15 €**.
# MAGIC - Ubisoft, positionné AAA, vise plutôt une fourchette entre **20 € et 60 €**.
# MAGIC
# MAGIC Le dataset permet donc de comprendre la structure du marché mais  
# MAGIC pas de comparer directement les AAA (rarement présents dans les données).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 Jeux en promotion (discount > 0)
# MAGIC
# MAGIC Nous observons la proportion de jeux qui appliquent des promotions.

# COMMAND ----------

discount_count = games_df.filter(F.col("discount") > 0).count()
total_games = games_df.count()

print(f"Jeux en promotion : {discount_count} / {total_games} "
      f"({discount_count/total_games*100:.2f}%)")

display(
    games_df.filter("discount > 0")
            .select("name", "price_eur", "initialprice_eur", "discount")
            .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Commentaire :
# MAGIC
# MAGIC - Environ **1 jeu sur 3** applique une promotion.
# MAGIC - Steam habitue sa communauté à acheter en Soldes.
# MAGIC - Un jeu Ubisoft doit intégrer une stratégie :
# MAGIC   - lancement **à plein tarif**
# MAGIC   - premières promotions **modérées** (-10% à -25%)
# MAGIC   - grosses promotions en période de Soldes Steam (-40% à -60%)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.3 Prix par genre

# COMMAND ----------

price_by_genre_df = (
    genres_exploded_df
    .groupBy("genre")
    .agg(
        F.count("*").alias("nb_games"),
        F.avg("price_eur").alias("avg_price_eur"),
        F.expr("percentile(price_eur, 0.5)").alias("median_price_eur")
    )
    .orderBy(F.desc("median_price_eur"))
)

display(price_by_genre_df)
price_by_genre_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Genres les **plus chers** (médiane la plus élevée) :
# MAGIC
# MAGIC - RPG
# MAGIC - Simulation
# MAGIC - Strategy
# MAGIC - Racing
# MAGIC
# MAGIC → Jeux complexes, souvent premium.
# MAGIC
# MAGIC Genres **moins chers** :
# MAGIC
# MAGIC - Indie
# MAGIC - Casual
# MAGIC - Free to Play
# MAGIC
# MAGIC → Cibles grand public, modèle low-cost.
# MAGIC
# MAGIC Ubisoft se positionne naturellement dans :
# MAGIC **Action / Adventure / RPG / Strategy**,  
# MAGIC donc dans la partie “premium” du marché

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.4 Prix ↔ Succès (reviews)

# COMMAND ----------

price_success_df = (
    games_df
    .select("price_eur", "total_reviews", "positive_ratio")
    .filter("total_reviews > 0 AND price_eur > 0")
)

display(price_success_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interprétation :
# MAGIC
# MAGIC
# MAGIC - Les jeux entre **5 € et 20 €** génèrent **le plus de reviews**.
# MAGIC - Les jeux très chers (> 40 €) ont peu de reviews → ils ne sont pas leaders.
# MAGIC - Les blockbusters (ex : Terraria, Project Zomboid) sont autour de **9,99 € – 19,99 €**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.5 Prix moyen par période COVID
# MAGIC

# COMMAND ----------

covid_price_df = (
    games_df
    .groupBy("covid_period")
    .agg(
        F.count("*").alias("nb_games"),
        F.avg("price_eur").alias("avg_price_eur"),
        F.avg("discount").alias("avg_discount")
    )
)

display(covid_price_df)
covid_price_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Commentaire final :
# MAGIC
# MAGIC - Pendant la période **COVID (2019–2021)**, les sorties ont explosé (+24 000 jeux),
# MAGIC   mais les **prix moyens sont restés stables**.
# MAGIC - Les studios indie ont “inondé” Steam d’offres low-cost.
# MAGIC - Ubisoft peut se différencier par :
# MAGIC   - **la qualité**
# MAGIC   - **la profondeur**
# MAGIC   - **le cross-platform**
# MAGIC   - **un pricing premium maîtrisé**

# COMMAND ----------

# MAGIC %md
# MAGIC # 9. Insights Business pour Ubisoft 
# MAGIC Analyse stratégique basée sur les données Steam
# MAGIC
# MAGIC Dans cette section, on synthétise les enseignements majeurs du marché Steam
# MAGIC afin de proposer des recommandations concrètes pour Ubisoft.
# MAGIC
# MAGIC Ces insights s’appuient sur :
# MAGIC
# MAGIC l’analyse des genres (Section 6)
# MAGIC
# MAGIC les prix & promotions (Section 8)
# MAGIC
# MAGIC les tendances annuelles & COVID (Section 5)
# MAGIC
# MAGIC les plateformes (Section 5.1 et 6.6)
# MAGIC
# MAGIC les blockbusters (Section 6.5)

# COMMAND ----------

# MAGIC %md
# MAGIC Insight 1 — Steam est dominé par les jeux Indie & Action
# MAGIC
# MAGIC (mais les blockbusters restent concentrés dans les genres premium)
# MAGIC
# MAGIC Ce que disent les données :
# MAGIC
# MAGIC Top genres les plus présents :
# MAGIC
# MAGIC Indie (39 681 jeux)
# MAGIC
# MAGIC Action (23 759 jeux)
# MAGIC
# MAGIC Casual (22 086 jeux)
# MAGIC
# MAGIC Adventure (21 431 jeux)
# MAGIC
# MAGIC Les Indies représentent 40–45 % du catalogue → marché saturé.
# MAGIC
# MAGIC Mais côté blockbusters (> 50k reviews + > 90% positives) :
# MAGIC Genre	Nb blockbusters
# MAGIC Action	86
# MAGIC Indie	74
# MAGIC Adventure	59
# MAGIC Simulation	40
# MAGIC RPG	32
# MAGIC Strategy	31
# MAGIC
# MAGIC -- Les blockbusters se concentrent sur les six mêmes genres que ceux d’Ubisoft. -->
# MAGIC -- Donc Ubisoft reste aligné avec la zone “haut potentiel”, pas la zone low-cost.
# MAGIC
# MAGIC Recommandation Ubisoft
# MAGIC
# MAGIC ✔ Continuer à viser : Action / Adventure / RPG / Strategy / Simulation
# MAGIC
# MAGIC ✔ Positionnement premium = cohérent avec les genres qui génèrent le plus de succès
# MAGIC
# MAGIC ✔ Investir dans des mécaniques hybrides :
# MAGIC
# MAGIC   . Action + Survival
# MAGIC
# MAGIC   . RPG + Strategy
# MAGIC
# MAGIC   . Simulation + Narrative