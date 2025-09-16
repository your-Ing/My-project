#A installer
#!pip install pandas
#!pip install mysql-connector-python
#!pip install openpyxl
#!pip install configparser



#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de Contrôle des Factures PGOP -> JDE.
 test local XAMPP.


"""

# Importation des modules nécessaires
import pandas as pd  # Pour la manipulation de données et les opérations sur les DataFrames
import mysql.connector  # Pour la connexion à la base de données MySQL/MariaDB
from mysql.connector import Error  # Pour gérer les erreurs de connexion MySQL
import logging  # Pour la journalisation des événements et erreurs
from datetime import datetime  # Pour manipuler les dates et heures
import os  # Pour les opérations sur le système de fichiers
import smtplib  # Pour l'envoi d'emails via SMTP
from email.mime.multipart import MIMEMultipart  # Pour créer des emails multiparts
from email.mime.text import MIMEText  # Pour ajouter du texte aux emails
from email.mime.application import MIMEApplication  # Pour ajouter des pièces jointes aux emails
from configparser import ConfigParser  # Pour lire les fichiers de configuration

# --- CONFIGURATION ---
def load_config(config_file='config.ini'):
    """
    Charge le fichier de configuration.
    
    Args:
        config_file (str): Chemin vers le fichier de configuration. Par défaut 'config.ini'
    
    Returns:
        ConfigParser: Objet contenant les configurations lues
    """
    config = ConfigParser()
    config.read(config_file)
    return config

# Chargement de la configuration
config = load_config()

# Configuration du système de logging (journalisation)
logging.basicConfig(
    level=logging.INFO,  # Niveau de log : INFO (affiche les messages informatifs et plus graves)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Format des messages de log
    handlers=[  # Gestionnaires de log : fichier et console
        logging.FileHandler(f"facturation_control_{datetime.now().strftime('%Y%m%d_%H%M')}.log"),  # Écrit dans un fichier
        logging.StreamHandler()  # Affiche dans la console
    ]
)
logger = logging.getLogger('PGOP_JDE_Control')  # Crée un logger avec le nom spécifié

# Configuration de la connexion à la base de données MySQL à partir du fichier config
DB_CONFIG = {
    'host': config['database']['host'],  # Adresse du serveur de base de données
    'database': config['database']['database'],  # Nom de la base de données
    'user': config['database']['user'],  # Nom d'utilisateur pour la connexion
    'password': config['database']['password']  # Mot de passe pour la connexion
}

# Configuration pour l'envoi d'emails (exemple avec Gmail)
SMTP_SERVER = config['email']['smtp_server']  # Serveur SMTP (ex: smtp.gmail.com)
SMTP_PORT = int(config['email']['smtp_port'])  # Port SMTP (ex: 587)
SENDER_EMAIL = config['email']['sender_email']  # Email de l'expéditeur
SENDER_PASSWORD = config['email']['sender_password']  # Mot de passe de l'expéditeur
RECIPIENTS = config['email']['recipients'].split(',')  # Liste des destinataires (séparés par des virgules)

# Configuration du répertoire de sortie pour les fichiers générés
OUTPUT_DIR = "output"  # Nom du répertoire
os.makedirs(OUTPUT_DIR, exist_ok=True)  # Crée le répertoire s'il n'existe pas déjà

# --- FONCTIONS UTILITAIRES ---
def get_db_connection():
    """
    Établit et retourne une connexion à la base de données MySQL.
    
    Returns:
        Connection: Objet de connexion à la base de données
    
    Raises:
        Error: Si la connexion échoue
    """
    try:
        connection = mysql.connector.connect(**DB_CONFIG)  # Tente de se connecter avec les paramètres
        logger.info("Connexion à MySQL réussie.")  # Log en cas de succès
        return connection
    except Error as e:
        logger.error(f"Erreur de connexion à MySQL: {e}")  # Log en cas d'erreur
        raise  # Relance l'exception pour la gérer plus haut

def run_query(connection, query, params=None):
    """
    Exécute une requête SQL et retourne un DataFrame pandas.
    
    Args:
        connection: Connexion à la base de données
        query (str): Requête SQL à exécuter
        params (tuple, optional): Paramètres pour la requête paramétrée
    
    Returns:
        DataFrame: Résultat de la requête sous forme de DataFrame pandas
    """
    try:
        df = pd.read_sql(query, con=connection, params=params)  # Exécute la requête et récupère les données
        logger.info(f"Requête exécutée. {len(df)} lignes récupérées.")  # Log le nombre de lignes récupérées
        return df
    except Error as e:
        logger.error(f"Erreur MySQL lors de l'exécution de la requête: {e}\nQuery: {query}")  # Log les erreurs MySQL
        return pd.DataFrame()  # Retourne un DataFrame vide en cas d'erreur
    except Exception as e:
        logger.error(f"Erreur générale (pandas?) lors de l'exécution de la requête: {e}")  # Log les autres erreurs
        return pd.DataFrame()  # Retourne un DataFrame vide en cas d'erreur

def send_email(subject, body, attachment_path=None):
    """
    Envoie un email avec un rapport en pièce-jointe.
    
    Args:
        subject (str): Sujet de l'email
        body (str): Corps de l'email
        attachment_path (str, optional): Chemin vers le fichier à joindre
    """
    msg = MIMEMultipart()  # Crée un message multipart (peut contenir plusieurs parties)
    msg['From'] = SENDER_EMAIL  # Définit l'expéditeur
    msg['To'] = ", ".join(RECIPIENTS)  # Définit les destinataires (séparés par des virgules)
    msg['Subject'] = subject  # Définit le sujet
    msg.attach(MIMEText(body, 'plain'))  # Attache le corps du message en texte brut

    # Gestion de la pièce jointe si elle est spécifiée et existe
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:  # Ouvre le fichier en mode lecture binaire
            part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))  # Crée la partie MIME
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'  # Définit le nom du fichier joint
        msg.attach(part)  # Attache la pièce jointe au message
        logger.info(f"Fichier joint: {attachment_path}")  # Log l'ajout de la pièce jointe

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:  # Se connecte au serveur SMTP
            server.starttls()  # Active le chiffrement TLS
            server.login(SENDER_EMAIL, SENDER_PASSWORD)  # S'authentifie
            server.sendmail(SENDER_EMAIL, RECIPIENTS, msg.as_string())  # Envoie l'email
        logger.info(f"Email envoyé avec succès: {subject}")  # Log en cas de succès
    except Exception as e:
        logger.error(f"Erreur lors de l'envoi de l'email: {e}")  # Log en cas d'erreur

def save_to_excel(df, sheet_name, file_path):
    """
    Sauvegarde un DataFrame dans une feuille d'un fichier Excel.
    
    Args:
        df (DataFrame): DataFrame à sauvegarder
        sheet_name (str): Nom de la feuille Excel
        file_path (str): Chemin vers le fichier Excel
    """
    try:
        # Détermine le mode d'ouverture : ajout si le fichier existe, création sinon
        mode = 'a' if os.path.exists(file_path) else 'w'
        with pd.ExcelWriter(file_path, engine='openpyxl', mode=mode) as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)  # Écrit le DataFrame dans Excel
        logger.info(f"Feuille '{sheet_name}' sauvegardée dans {file_path}")  # Log en cas de succès
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde Excel ({sheet_name}): {e}")  # Log en cas d'erreur

# --- DEFINITION DES CONTROLES ---
def controle_1(conn, output_file):
    """
    Contrôle 1: Vérifie les factures non-transmises de LQ_FACTURA_B dans FCABFAC.
    
    Args:
        conn: Connexion à la base de données
        output_file (str): Chemin du fichier Excel de sortie
    
    Returns:
        str: Message de résumé du contrôle
    """
    logger.info("Début du Contrôle 1")  # Log le début du contrôle
    
    # Requête pour trouver les factures manquantes
    query = """
    SELECT IDINTERNO, NUMFACTURA, ESTADO, FECFACTURA
    FROM LQ_FACTURA_B
    WHERE ESTADO IN ('R','A','C','N','F','G','H','K','L','E','J','V')  # États spécifiques
    AND FECFACTURA LIKE '%%/07/24'  # Factures de juillet 2024
    AND BFUSIC IS NULL  # Champ BFUSIC non renseigné
    AND IDINTERNO NOT IN (SELECT IDFACTURA FROM FCABFAC)  # Non présentes dans FCABFAC
    ORDER BY IDINTERNO  # Tri par ID
    """
    df_manquantes = run_query(conn, query)  # Exécute la requête

    if not df_manquantes.empty:  # Si des factures manquantes sont trouvées
        # Séparation des factures par état
        df_L = df_manquantes[df_manquantes['ESTADO'] == 'L']  # Factures avec état 'L'
        df_F = df_manquantes[df_manquantes['ESTADO'] == 'F']  # Factures avec état 'F'
        df_autres = df_manquantes[(df_manquantes['ESTADO'] != 'F') & (df_manquantes['ESTADO'] != 'L')]  # Autres états

        # Sauvegarde des résultats dans différentes feuilles Excel
        save_to_excel(df_manquantes, "Contrôle1_Toutes_Manquantes", output_file)
        save_to_excel(df_L, "Contrôle1_ETAT_L", output_file)
        save_to_excel(df_F, "Contrôle1_ETAT_F", output_file)
        save_to_excel(df_autres, "Contrôle1_Autres_Etats", output_file)

        logger.warning(f"Contrôle 1 ALERTE: {len(df_manquantes)} factures manquantes.")  # Log d'alerte
        return f"[ALERTE] Contrôle 1: {len(df_manquantes)} factures manquantes."  # Message de retour
    else:
        logger.info("Contrôle 1 OK: Aucune facture manquante.")  # Log positif
        return "[OK] Contrôle 1: Aucune anomalie."  # Message de retour

def controle_2(conn, output_file):
    """
    Contrôle 2: Vérifie les factures non-transmises de FCABFAC dans F58PGOP1.
    
    Args:
        conn: Connexion à la base de données
        output_file (str): Chemin du fichier Excel de sortie
    
    Returns:
        str: Message de résumé du contrôle
    """
    logger.info("Début du Contrôle 2")  # Log le début du contrôle
    
    # Requête pour récupérer les factures de FCABFAC
    query_pgop = """
    SELECT k.IDFACTURA, k.NUMFACTURA, k.CABDSP, k.FECFACTURA, k.TIPOSERIE, k.IMPNET, l.ESTADO
    FROM FCABFAC k
    INNER JOIN LQ_FACTURA_B l ON k.IDFACTURA = l.IDINTERNO  # Jointure avec LQ_FACTURA_B
    WHERE l.FECFACTURA LIKE '%%/07/24'  # Factures de juillet 2024
    AND l.BFUSIC IS NULL  # Champ BFUSIC non renseigné
    AND k.CABDSP = 'Y'  # Seulement les factures avec CABDSP = 'Y'
    ORDER BY k.NUMFACTURA  # Tri par numéro de facture
    """
    df_pgop = run_query(conn, query_pgop)  # Exécute la requête

    # Requête pour récupérer les factures de F58PGOP1 (JDE)
    query_jde = """
    SELECT PGCCID, PGASID, PGLOT, PGBP01, PG74UAMT1/100 as Montant
    FROM F58PGOP1
    WHERE PGLOT LIKE '07/%%/2024%%'  # Lots de juillet 2024
    ORDER BY PGASID  # Tri par ID
    """
    df_jde = run_query(conn, query_jde)  # Exécute la requête

    if not df_pgop.empty:  # Si des factures sont trouvées dans FCABFAC
        # Crée un set des IDs présents dans JDE pour une recherche rapide
        id_dans_jde = set(df_jde['PGASID'].astype(int).unique()) if not df_jde.empty else set()
        
        # Trouve les factures de FCABFAC qui ne sont pas dans JDE
        df_manquantes = df_pgop[~df_pgop['IDFACTURA'].astype(int).isin(id_dans_jde)]

        if not df_manquantes.empty:  # Si des factures manquantes sont trouvées
            save_to_excel(df_manquantes, "Contrôle2_Manquantes", output_file)  # Sauvegarde les résultats
            logger.warning(f"Contrôle 2 ALERTE: {len(df_manquantes)} factures non transmises.")  # Log d'alerte
            return f"[ALERTE] Contrôle 2: {len(df_manquantes)} factures non transmises."  # Message de retour
        else:
            logger.info("Contrôle 2 OK: Toutes les factures sont dans JDE.")  # Log positif
            return "[OK] Contrôle 2: Aucune anomalie."  # Message de retour
    else:
        logger.info("Contrôle 2: Aucune facture à vérifier.")  # Log informatif
        return "[INFO] Contrôle 2: Aucune facture trouvée."  # Message de retour

def controle_3(conn, output_file):
    """
    Contrôle 3: Vérifie les factures non-transmises de F58PGOP1 dans F03B11 (comptabilité).
    
    Args:
        conn: Connexion à la base de données
        output_file (str): Chemin du fichier Excel de sortie
    
    Returns:
        str: Message de résumé du contrôle
    """
    logger.info("Début du Contrôle 3")  # Log le début du contrôle
    
    # Requête pour trouver les factures manquantes en comptabilité
    query = """
    SELECT PGCCID, PGASID, PGLOT, PGBP01, PG74UAMT1, PGEV01
    FROM F58PGOP1
à    WHERE PGLOT LIKE '%%07/%%/2024%%'  # Lots de juillet 2024
    AND PGCCID NOT IN (SELECT RPDOC FROM F03B11)  # Non présentes en comptabilité
    ORDER BY PGCCID  # Tri par ID
    """
    df_manquantes = run_query(conn, query)  # Exécute la requête

    if not df_manquantes.empty:  # Si des factures manquantes sont trouvées
        save_to_excel(df_manquantes, "Contrôle3_Manquantes", output_file)  # Sauvegarde les résultats
        logger.warning(f"Contrôle 3 ALERTE: {len(df_manquantes)} factures manquantes en compta.")  # Log d'alerte
        return f"[ALERTE] Contrôle 3: {len(df_manquantes)} factures manquantes en compta."  # Message de retour
    else:
        logger.info("Contrôle 3 OK: Toutes les factures sont en compta.")  # Log positif
        return "[OK] Contrôle 3: Aucune anomalie."  # Message de retour

def controle_4(conn, output_file):
    """
    Contrôle 4: Vérifie les factures non-transmises avec CODE 4.
    
    Args:
        conn: Connexion à la base de données
        output_file (str): Chemin du fichier Excel de sortie
    
    Returns:
        str: Message de résumé du contrôle
    """
    logger.info("Début du Contrôle 4")  # Log le début du contrôle
    
    # Requête pour trouver les factures avec erreur Code 4
    query = """
    SELECT PGCCID, PGASID, PGLOT, PGBP01, PG74UAMT1, PGEV01
    FROM F58PGOP1
    WHERE PGLOT LIKE '%%07/%%/2024%%'  # Lots de juillet 2024
    AND PGCCID NOT IN (SELECT RPDOC FROM F03B11)  # Non présentes en comptabilité
    AND PGEV01 = 4  # Avec code d'erreur 4
    ORDER BY PGCCID  # Tri par ID
    """
    df_code4 = run_query(conn, query)  # Exécute la requête

    if not df_code4.empty:  # Si des factures avec code 4 sont trouvées
        save_to_excel(df_code4, "Contrôle4_Code4", output_file)  # Sauvegarde les résultats
        
        # Récupère les IDs pour une requête supplémentaire
        ids = df_code4['PGCCID'].tolist()
        if ids:
            # Requête pour obtenir les informations clients des factures problématiques
            query_client = """
            SELECT IDINTERNO, NUMFACTURA, NOMUSU
            FROM LQ_FACTURA_B
            WHERE IDINTERNO IN (%s)  # Liste des IDs problématiques
            ORDER BY NOMUSU  # Tri par nom d'utilisateur
            """ % ','.join(['%s'] * len(ids))  # Construction dynamique de la requête
            
            df_clients = run_query(conn, query_client, params=ids)  # Exécute la requête
            if not df_clients.empty:
                save_to_excel(df_clients, "Contrôle4_Clients", output_file)  # Sauvegarde les résultats

        # Requête pour vérifier les informations IFU (Identification Fiscale Unique)
        query_ifu = """
        SELECT ABALPH, ABTAX
        FROM F0101
        WHERE ABALPH LIKE '%%PUMA%%'  # Recherche des enregistrements contenant "PUMA"
        LIMIT 10  # Limite à 10 résultats
        """
        df_ifu = run_query(conn, query_ifu)  # Exécute la requête
        if not df_ifu.empty:
            save_to_excel(df_ifu, "Contrôle4_IFU", output_file)  # Sauvegarde les résultats

        logger.warning(f"Contrôle 4 ALERTE: {len(df_code4)} factures avec erreur Code 4.")  # Log d'alerte
        return f"[ALERTE] Contrôle 4: {len(df_code4)} factures avec erreur Code 4."  # Message de retour
    else:
        logger.info("Contrôle 4 OK: Aucune erreur Code 4.")  # Log positif
        return "[OK] Contrôle 4: Aucune anomalie."  # Message de retour

def controle_5(conn, output_file):
    """
    Contrôle 5: Réconciliation des montants entre les différentes tables.
    
    Args:
        conn: Connexion à la base de données
        output_file (str): Chemin du fichier Excel de sortie
    
    Returns:
        str: Message de résumé du contrôle
    """
    logger.info("Début du Contrôle 5")  # Log le début du contrôle
    
    # Requête pour récupérer les montants depuis LQ_FACTURA_B
    query_pgop = """
    SELECT IDINTERNO, NUMFACTURA, IMPNET, IMPIVA, IMPTOT
    FROM LQ_FACTURA_B
    WHERE FECFACTURA LIKE '%%/07/24'  # Factures de juillet 2024
    AND BFUSIC IS NULL  # Champ BFUSIC non renseigné
    AND ESTADO IN ('R','A','C','N','F','G','H','K','L','E','J','V')  # États spécifiques
    ORDER BY NUMFACTURA  # Tri par numéro de facture
    """
    df_pgop = run_query(conn, query_pgop)  # Exécute la requête
    
    # Requête pour récupérer les montants depuis F03B11 (comptabilité)
    query_jde = """
    SELECT RPDOC, RPATXA, RPSTAM, RPAG
    FROM F03B11
    WHERE RPVR01 LIKE '%%|PAC|2024%%'  # Documents de l'année 2024
    ORDER BY RPDOC  # Tri par document
    """
    df_jde = run_query(conn, query_jde)  # Exécute la requête

    if not df_pgop.empty and not df_jde.empty:  # Si des données sont disponibles dans les deux tables
        # Jointure des deux DataFrames sur l'ID de document
        df_compare = pd.merge(
            df_pgop, 
            df_jde, 
            left_on='IDINTERNO', 
            right_on='RPDOC', 
            how='inner'  # Jointure interne (seulement les correspondances)
        )
        
        # Recherche des écarts de montant
        df_ecarts = df_compare[
            (df_compare['IMPNET'] != df_compare['RPATXA']) |  # Écart sur montant net
            (df_compare['IMPIVA'] != df_compare['RPSTAM']) |  # Écart sur TVA
            (df_compare['IMPTOT'] != df_compare['RPAG'])  # Écart sur montant total
        ]
        
        if not df_ecarts.empty:  # Si des écarts sont trouvés
            save_to_excel(df_ecarts, "Contrôle5_Ecarts", output_file)  # Sauvegarde les résultats
            logger.warning(f"Contrôle 5 ALERTE: {len(df_ecarts)} écarts de montant.")  # Log d'alerte
            return f"[ALERTE] Contrôle 5: {len(df_ecarts)} écarts de montant."  # Message de retour
        else:
            logger.info("Contrôle 5 OK: Aucun écart de montant.")  # Log positif
            return "[OK] Contrôle 5: Aucun écart."  # Message de retour
    else:
        logger.warning("Contrôle 5: Données insuffisantes.")  # Log d'avertissement
        return "[WARNING] Contrôle 5: Données insuffisantes."  # Message de retour

# --- POINT D'ENTREE DU SCRIPT ---
def main():
    """
    Fonction principale qui orchestre l'exécution de tous les contrôles.
    """
    logger.info("=== Démarrage du script de contrôle facturation ===")  # Log de début d'exécution
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")  # Timestamp pour le nom du fichier
    rapport_file = os.path.join(OUTPUT_DIR, f"rapport_controles_{timestamp}.xlsx")  # Chemin du fichier de rapport

    resume_controles = []  # Liste pour stocker les résumés de chaque contrôle

    try:
        conn = get_db_connection()  # Établit la connexion à la base de données
        # Exécute tous les contrôles et stocke les résultats
        resume_controles.append(controle_1(conn, rapport_file))
        resume_controles.append(controle_2(conn, rapport_file))
        resume_controles.append(controle_3(conn, rapport_file))
        resume_controles.append(controle_4(conn, rapport_file))
        resume_controles.append(controle_5(conn, rapport_file))
        conn.close()  # Ferme la connexion à la base de données
        logger.info("Tous les contrôles terminés. Connexion fermée.")  # Log de fin d'exécution

    except Exception as e:
        error_msg = f"Erreur critique: {e}"  # Message d'erreur
        logger.error(error_msg)  # Log l'erreur
        resume_controles.append(f"[ERREUR] {e}")  # Ajoute l'erreur au résumé

    # Construction du corps de l'email
    email_body = f"Résumé des contrôles du {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
    email_body += "\n".join(resume_controles)  # Ajoute les résumés de chaque contrôle
    email_body += f"\n\nLe rapport détaillé est en pièce-jointe."  # Mention de la pièce jointe

    # Détermine le sujet de l'email en fonction des résultats
    if any("ALERTE" in res or "ERREUR" in res for res in resume_controles):
        subject = f"[ALERTE] Rapport Contrôles - {timestamp}"  # Sujet d'alerte
    else:
        subject = f"[OK] Rapport Contrôles - {timestamp}"  # Sujet normal

    # Envoi de l'email avec ou sans pièce jointe
    if os.path.exists(rapport_file):
        send_email(subject, email_body, rapport_file)  # Avec pièce jointe
    else:
        send_email(subject, email_body)  # Sans pièce jointe
    
    logger.info("=== Exécution terminée ===")  # Log de fin d'exécution

# Point d'entrée du script
if __name__ == "__main__":
    main()  # Exécute la fonction principale si le script est lancé directement