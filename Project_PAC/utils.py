import os
import logging
import pandas as pd
import mysql.connector
from mysql.connector import Error
from configparser import ConfigParser
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# -----------------------
# CONFIGURATION
# -----------------------
def load_config(config_file='config.ini'):
    config = ConfigParser()
    config.read(config_file)
    return config


def get_db_connection():
    try:
        config = load_config()
        DB_CONFIG = {
            'host': config['database']['host'],
            'database': config['database']['database'],
            'user': config['database']['user'],
            'password': config['database']['password']
        }
        connection = mysql.connector.connect(**DB_CONFIG)
        logging.info("Connexion à MySQL réussie.")
        return connection
    except Error as e:
        logging.error(f"Erreur de connexion à MySQL: {e}")
        raise


def run_query(connection, query, params=None):
    try:
        df = pd.read_sql(query, con=connection, params=params)
        logging.info(f"Requête exécutée. {len(df)} lignes récupérées.")
        return df
    except Exception as e:
        logging.error(f"Erreur lors de l'exécution de la requête: {e}")
        return pd.DataFrame()


def save_to_excel(df, sheet_name, file_path):
    try:
        mode = 'a' if os.path.exists(file_path) else 'w'
        with pd.ExcelWriter(file_path, engine='openpyxl', mode=mode) as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        logging.info(f"Feuille '{sheet_name}' sauvegardée dans {file_path}")
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde Excel ({sheet_name}): {e}")


# -----------------------
# ENVOI EMAIL
# -----------------------
def send_email(subject, body_html, attachment_path=None, recipients=None):
    config = load_config()
    SMTP_SERVER = config['email']['smtp_server']
    SMTP_PORT = int(config['email']['smtp_port'])
    SENDER_EMAIL = config['email']['sender_email']
    SENDER_PASSWORD = config['email']['sender_password']

    if recipients is None:
        logging.error("Aucun destinataire fourni.")
        return

    msg = MIMEMultipart('alternative')
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject

    # Corps du message HTML
    msg.attach(MIMEText(body_html, 'html'))

    # Pièce jointe
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
        msg.attach(part)
        logging.info(f"Fichier joint: {attachment_path}")

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipients, msg.as_string())
        logging.info(f"Email envoyé à {recipients} avec succès : {subject}")
    except Exception as e:
        logging.error(f"Erreur lors de l'envoi de l'email : {e}")
