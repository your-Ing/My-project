#A installer
#!pip install pandas
#!pip install mysql-connector-python
#!pip install openpyxl
#!pip install configparser


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de Contrôle des Factures PGOP -> JDE.
Avec génération de tableaux HTML pour les emails.

"""

# Importation des modules nécessaires
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from configparser import ConfigParser

# --- CONFIGURATION ---
def load_config(config_file='config.ini'):
    """
    Charge le fichier de configuration.
    """
    config = ConfigParser()
    config.read(config_file)
    return config

# Chargement de la configuration
config = load_config()

# Configuration du système de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"facturation_control_{datetime.now().strftime('%Y%m%d_%H%M')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('PGOP_JDE_Control')

# Configuration de la connexion à la base de données
DB_CONFIG = {
    'host': config['database']['host'],
    'database': config['database']['database'],
    'user': config['database']['user'],
    'password': config['database']['password']
}

# Configuration pour l'envoi d'emails
SMTP_SERVER = config['email']['smtp_server']
SMTP_PORT = int(config['email']['smtp_port'])
SENDER_EMAIL = config['email']['sender_email']
SENDER_PASSWORD = config['email']['sender_password']
RECIPIENTS = config['email']['recipients'].split(',')

# Configuration du répertoire de sortie
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- FONCTIONS UTILITAIRES ---
def get_db_connection():
    """
    Établit et retourne une connexion à la base de données MySQL.
    """
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        logger.info("Connexion à MySQL réussie.")
        return connection
    except Error as e:
        logger.error(f"Erreur de connexion à MySQL: {e}")
        raise

def run_query(connection, query, params=None):
    """
    Exécute une requête SQL et retourne un DataFrame pandas.
    """
    try:
        df = pd.read_sql(query, con=connection, params=params)
        logger.info(f"Requête exécutée. {len(df)} lignes récupérées.")
        return df
    except Error as e:
        logger.error(f"Erreur MySQL lors de l'exécution de la requête: {e}\nQuery: {query}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erreur générale (pandas?) lors de l'exécution de la requête: {e}")
        return pd.DataFrame()

def generate_html_table(df, title="", etat=None):
    """
    Génère un tableau HTML compatible avec les clients email.
    """
    if df.empty:
        return f"<h3 style='color: #2c3e50;'>{title}</h3><p>Aucune facture {etat if etat else ''} en attente</p>"
    
    # Renommer les colonnes
    df_display = df.rename(columns={
        'IDINTERNO': 'IDFACTURE',
        'NUMFACTURA': 'NUMFACTURE',
        'ESTADO': 'ETAT',
        'FECFACTURA': 'DATE'
    })
    
    # HTML optimisé pour les emails - EN-TÊTES EN GRIS
    html_table = f"""
    <div style="margin: 20px 0; padding: 15px; background-color: #f9f9f9; border-left: 4px solid #95a5a6;">
        <h3 style="color: #2c3e50; margin: 0 0 15px 0; font-family: Arial, sans-serif;">{title}</h3>
        <table width="100%" border="0" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif; font-size: 14px; background-color: white;">
            <tr style="background-color: #95a5a6; color: white;">
                <th align="left" style="border: 1px solid #ddd; padding: 12px;"><strong>IDFACTURE</strong></th>
                <th align="left" style="border: 1px solid #ddd; padding: 12px;"><strong>NUMFACTURE</strong></th>
                <th align="left" style="border: 1px solid #ddd; padding: 12px;"><strong>ETAT</strong></th>
                <th align="left" style="border: 1px solid #ddd; padding: 12px;"><strong>DATE</strong></th>
            </tr>
    """
    
    # Alternance de couleurs pour les lignes
    for i, (_, row) in enumerate(df_display.iterrows()):
        bg_color = "#f2f2f2" if i % 2 == 0 else "#ffffff"
        html_table += f"""
            <tr style="background-color: {bg_color};">
                <td style="border: 1px solid #ddd; padding: 10px;">{row['IDFACTURE']}</td>
                <td style="border: 1px solid #ddd; padding: 10px;">{row['NUMFACTURE']}</td>
                <td style="border: 1px solid #ddd; padding: 10px;"><strong>{row['ETAT']}</strong></td>
                <td style="border: 1px solid #ddd; padding: 10px;">{row['DATE']}</td>
            </tr>
        """
    
    html_table += f"""
        </table>
        <p style="margin: 10px 0 0 0; color: #666; font-style: italic; font-family: Arial, sans-serif;">
            Total: <strong>{len(df)}</strong> facture(s)
        </p>
    </div>
    """
    return html_table

def send_email(subject, body, attachment_path=None, html_body=None):
    """
    Envoie un email avec option HTML - VERSION CORRIGÉE.
    """
    msg = MIMEMultipart('mixed')
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECIPIENTS)
    msg['Subject'] = subject
    
    # Créer la partie alternative (texte + HTML)
    if html_body:
        alternative = MIMEMultipart('alternative')
        
        # Partie texte brut
        text_part = MIMEText(body, 'plain', 'utf-8')
        alternative.attach(text_part)
        
        # Partie HTML
        html_part = MIMEText(html_body, 'html', 'utf-8')
        alternative.attach(html_part)
        
        msg.attach(alternative)
    else:
        # Seulement texte brut
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
    
    # Pièce-jointe (fichier Excel)
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
        msg.attach(part)
        logger.info(f"Fichier joint: {attachment_path}")

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECIPIENTS, msg.as_string())
        logger.info(f"Email envoyé avec succès: {subject}")
    except Exception as e:
        logger.error(f"Erreur lors de l'envoi de l'email: {e}")

def save_to_excel(df, sheet_name, file_path):
    """
    Sauvegarde un DataFrame dans une feuille d'un fichier Excel.
    """
    try:
        mode = 'a' if os.path.exists(file_path) else 'w'
        with pd.ExcelWriter(file_path, engine='openpyxl', mode=mode) as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        logger.info(f"Feuille '{sheet_name}' sauvegardée dans {file_path}")
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde Excel ({sheet_name}): {e}")

# --- DEFINITION DES CONTROLES ---
def controle_1(conn, output_file):
    """
    Contrôle 1: Vérifie les factures non-transmises de LQ_FACTURA_B dans FCABFAC.
    Retourne tous les DataFrames pour génération des tableaux email.
    """
    logger.info("Début du Contrôle 1")
    
    query = """
    SELECT IDINTERNO, NUMFACTURA, ESTADO, FECFACTURA
    FROM LQ_FACTURA_B
    WHERE ESTADO IN ('R','A','C','N','F','G','H','K','L','E','J','V')
    AND FECFACTURA LIKE '%%/07/24'
    AND BFUSIC IS NULL
    AND IDINTERNO NOT IN (SELECT IDFACTURA FROM FCABFAC)
    ORDER BY ESTADO, IDINTERNO
    """
    df_manquantes = run_query(conn, query)

    if not df_manquantes.empty:
        # Séparation des factures par état
        df_L = df_manquantes[df_manquantes['ESTADO'] == 'L']
        df_F = df_manquantes[df_manquantes['ESTADO'] == 'F']
        df_autres = df_manquantes[~df_manquantes['ESTADO'].isin(['L', 'F'])]

        # Sauvegarde Excel
        save_to_excel(df_manquantes, "Contrôle1_Toutes_Manquantes", output_file)
        save_to_excel(df_L, "Contrôle1_ETAT_L", output_file)
        save_to_excel(df_F, "Contrôle1_ETAT_F", output_file)
        save_to_excel(df_autres, "Contrôle1_Autres_Etats", output_file)

        logger.warning(f"Contrôle 1 ALERTE: {len(df_manquantes)} factures manquantes.")
        return f"[ALERTE] Contrôle 1: {len(df_manquantes)} factures manquantes.", df_L, df_F, df_autres, df_manquantes
    else:
        logger.info("Contrôle 1 OK: Aucune facture manquante.")
        return "[OK] Contrôle 1: Aucune anomalie.", pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def controle_2(conn, output_file):
    """
    Contrôle 2: Vérifie les factures non-transmises de FCABFAC dans F58PGOP1.
    """
    logger.info("Début du Contrôle 2")
    
    query_pgop = """
    SELECT k.IDFACTURA, k.NUMFACTURA, k.CABDSP, k.FECFACTURA, k.TIPOSERIE, k.IMPNET, l.ESTADO
    FROM FCABFAC k
    INNER JOIN LQ_FACTURA_B l ON k.IDFACTURA = l.IDINTERNO
    WHERE l.FECFACTURA LIKE '%%/07/24'
    AND l.BFUSIC IS NULL
    AND k.CABDSP = 'Y'
    ORDER BY k.NUMFACTURA
    """
    df_pgop = run_query(conn, query_pgop)

    query_jde = """
    SELECT PGCCID, PGASID, PGLOT, PGBP01, PG74UAMT1/100 as Montant
    FROM F58PGOP1
    WHERE PGLOT LIKE '07/%%/2024%%'
    ORDER BY PGASID
    """
    df_jde = run_query(conn, query_jde)

    if not df_pgop.empty:
        id_dans_jde = set(df_jde['PGASID'].astype(int).unique()) if not df_jde.empty else set()
        df_manquantes = df_pgop[~df_pgop['IDFACTURA'].astype(int).isin(id_dans_jde)]

        if not df_manquantes.empty:
            save_to_excel(df_manquantes, "Contrôle2_Manquantes", output_file)
            logger.warning(f"Contrôle 2 ALERTE: {len(df_manquantes)} factures non transmises.")
            return f"[ALERTE] Contrôle 2: {len(df_manquantes)} factures non transmises."
        else:
            logger.info("Contrôle 2 OK: Toutes les factures sont dans JDE.")
            return "[OK] Contrôle 2: Aucune anomalie."
    else:
        logger.info("Contrôle 2: Aucune facture à vérifier.")
        return "[INFO] Contrôle 2: Aucune facture trouvée."

def controle_3(conn, output_file):
    """
    Contrôle 3: Vérifie les factures non-transmises de F58PGOP1 dans F03B11 (comptabilité).
    """
    logger.info("Début du Contrôle 3")
    
    query = """
    SELECT PGCCID, PGASID, PGLOT, PGBP01, PG74UAMT1, PGEV01
    FROM F58PGOP1
    WHERE PGLOT LIKE '%%07/%%/2024%%'
    AND PGCCID NOT IN (SELECT RPDOC FROM F03B11)
    ORDER BY PGCCID
    """
    df_manquantes = run_query(conn, query)

    if not df_manquantes.empty:
        save_to_excel(df_manquantes, "Contrôle3_Manquantes", output_file)
        logger.warning(f"Contrôle 3 ALERTE: {len(df_manquantes)} factures manquantes en compta.")
        return f"[ALERTE] Contrôle 3: {len(df_manquantes)} factures manquantes en compta."
    else:
        logger.info("Contrôle 3 OK: Toutes les factures sont en compta.")
        return "[OK] Contrôle 3: Aucune anomalie."

def controle_4(conn, output_file):
    """
    Contrôle 4: Vérifie les factures non-transmises avec CODE 4.
    """
    logger.info("Début du Contrôle 4")
    
    query = """
    SELECT PGCCID, PGASID, PGLOT, PGBP01, PG74UAMT1, PGEV01
    FROM F58PGOP1
    WHERE PGLOT LIKE '%%07/%%/2024%%'
    AND PGCCID NOT IN (SELECT RPDOC FROM F03B11)
    AND PGEV01 = 4
    ORDER BY PGCCID
    """
    df_code4 = run_query(conn, query)

    if not df_code4.empty:
        save_to_excel(df_code4, "Contrôle4_Code4", output_file)
        
        ids = df_code4['PGCCID'].tolist()
        if ids:
            query_client = """
            SELECT IDINTERNO, NUMFACTURA, NOMUSU
            FROM LQ_FACTURA_B
            WHERE IDINTERNO IN (%s)
            ORDER BY NOMUSU
            """ % ','.join(['%s'] * len(ids))
            
            df_clients = run_query(conn, query_client, params=ids)
            if not df_clients.empty:
                save_to_excel(df_clients, "Contrôle4_Clients", output_file)

        query_ifu = """
        SELECT ABALPH, ABTAX
        FROM F0101
        WHERE ABALPH LIKE '%%PUMA%%'
        LIMIT 10
        """
        df_ifu = run_query(conn, query_ifu)
        if not df_ifu.empty:
            save_to_excel(df_ifu, "Contrôle4_IFU", output_file)

        logger.warning(f"Contrôle 4 ALERTE: {len(df_code4)} factures avec erreur Code 4.")
        return f"[ALERTE] Contrôle 4: {len(df_code4)} factures avec erreur Code 4."
    else:
        logger.info("Contrôle 4 OK: Aucune erreur Code 4.")
        return "[OK] Contrôle 4: Aucune anomalie."

def controle_5(conn, output_file):
    """
    Contrôle 5: Réconciliation des montants entre les différentes tables.
    """
    logger.info("Début du Contrôle 5")
    
    query_pgop = """
    SELECT IDINTERNO, NUMFACTURA, IMPNET, IMPIVA, IMPTOT
    FROM LQ_FACTURA_B
    WHERE FECFACTURA LIKE '%%/07/24'
    AND BFUSIC IS NULL
    AND ESTADO IN ('R','A','C','N','F','G','H','K','L','E','J','V')
    ORDER BY NUMFACTURA
    """
    df_pgop = run_query(conn, query_pgop)
    
    query_jde = """
    SELECT RPDOC, RPATXA, RPSTAM, RPAG
    FROM F03B11
    WHERE RPVR01 LIKE '%%|PAC|2024%%'
    ORDER BY RPDOC
    """
    df_jde = run_query(conn, query_jde)

    if not df_pgop.empty and not df_jde.empty:
        df_compare = pd.merge(
            df_pgop, 
            df_jde, 
            left_on='IDINTERNO', 
            right_on='RPDOC', 
            how='inner'
        )
        
        df_ecarts = df_compare[
            (df_compare['IMPNET'] != df_compare['RPATXA']) |
            (df_compare['IMPIVA'] != df_compare['RPSTAM']) |
            (df_compare['IMPTOT'] != df_compare['RPAG'])
        ]
        
        if not df_ecarts.empty:
            save_to_excel(df_ecarts, "Contrôle5_Ecarts", output_file)
            logger.warning(f"Contrôle 5 ALERTE: {len(df_ecarts)} écarts de montant.")
            return f"[ALERTE] Contrôle 5: {len(df_ecarts)} écarts de montant."
        else:
            logger.info("Contrôle 5 OK: Aucun écart de montant.")
            return "[OK] Contrôle 5: Aucun écart."
    else:
        logger.warning("Contrôle 5: Données insuffisantes.")
        return "[WARNING] Contrôle 5: Données insuffisantes."

# --- POINT D'ENTREE DU SCRIPT ---
def main():
    logger.info("=== Démarrage du script de contrôle facturation ===")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    rapport_file = os.path.join(OUTPUT_DIR, f"rapport_controles_{timestamp}.xlsx")

    resume_controles = []
    html_tables = ""

    try:
        conn = get_db_connection()
        
        # Contrôle 1 - avec retour de tous les DataFrames
        result_msg, df_L, df_F, df_autres, df_toutes = controle_1(conn, rapport_file)
        resume_controles.append(result_msg)
        
        # DEBUG: Loguer ce qui a été trouvé
        logger.info(f"Factures L: {len(df_L)}, F: {len(df_F)}, autres: {len(df_autres)}, total: {len(df_toutes)}")
        
        
        # Générer les tableaux HTML
        if not df_toutes.empty:
            html_tables = """
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Rapport Factures</title>
            </head>
            <body style="margin: 0; padding: 20px; font-family: Arial, sans-serif; background-color: #f4f4f4;">
                <div style="max-width: 900px; margin: 0 auto; background-color: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                    <h1 style="color: #2c3e50; text-align: center; border-bottom: 3px solid #95a5a6; padding-bottom: 15px; margin-bottom: 30px;">
                        📊 RAPPORT FACTURES EN ATTENTE
                    </h1>
                    <p style="color: #666; text-align: center; margin-bottom: 30px;">
                        Généré le: """ + datetime.now().strftime('%d/%m/%Y à %H:%M') + """
                    </p>
            """
            
            if not df_L.empty:
                html_tables += generate_html_table(df_L, "📋 Factures à l'état 'L'")
            if not df_F.empty:
                html_tables += generate_html_table(df_F, "📋 Factures à l'état 'F'")
            if not df_autres.empty:
                autres_etats = ", ".join(df_autres['ESTADO'].unique())
                html_tables += generate_html_table(df_autres, f"📋 Factures autres états ({autres_etats})")
            
            html_tables += generate_html_table(df_toutes, "📊 Récapitulatif général")
            
            html_tables += """
                    <div style="margin-top: 30px; padding: 15px; background-color: #ecf0f1; border-radius: 5px; border-left: 4px solid #95a5a6;">
                        <p style="margin: 0; color: #2c3e50;">
                            <strong>ℹ️ Note:</strong> Le rapport détaillé est disponible en pièce-jointe (fichier Excel).
                        </p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            # Sauvegarder le HTML pour inspection
            with open(os.path.join(OUTPUT_DIR, f"email_content_{timestamp}.html"), "w", encoding="utf-8") as f:
                f.write(html_tables)
            logger.info(f"Contenu HTML sauvegardé pour inspection")
    
        
        # Continuer avec les autres contrôles...
        resume_controles.append(controle_2(conn, rapport_file))
        resume_controles.append(controle_3(conn, rapport_file))
        resume_controles.append(controle_4(conn, rapport_file))
        resume_controles.append(controle_5(conn, rapport_file))
        
        conn.close()
        logger.info("Tous les contrôles terminés. Connexion fermée.")

    except Exception as e:
        error_msg = f"Erreur critique: {e}"
        logger.error(error_msg)
        resume_controles.append(f"[ERREUR] {e}")

    # Construction du corps de l'email
    email_body = f"Résumé des contrôles du {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
    email_body += "\n".join(resume_controles)
    email_body += f"\n\nLe rapport détaillé est en pièce-jointe."

    # DEBUG: Loguer le contenu HTML
    if html_tables:
        logger.info(f"Longueur du HTML généré: {len(html_tables)} caractères")
    else:
        logger.warning("Aucun contenu HTML généré")

    # Déterminer le sujet
    if any("ALERTE" in res or "ERREUR" in res for res in resume_controles):
        subject = f"[ALERTE] Factures en attente - {timestamp}"
    else:
        subject = f"[OK] Rapport Contrôles - {timestamp}"

    # Envoyer l'email
    send_email(subject, email_body, rapport_file, html_tables)
    
    logger.info("=== Exécution terminée ===")

# Point d'entrée du script
if __name__ == "__main__":
    main()