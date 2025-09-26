import logging
import os
from utils import run_query, save_to_excel

logger = logging.getLogger("PGOP_JDE_Control")

def controle_1(conn, year, month, output_file):
    """
    Contrôle 1: Vérifie les factures non-transmises de LQ_FACTURA_B dans FCABFAC.

    Args:
        conn: connexion à la base de données
        year (str): année AAAA
        month (str): mois MM
        output_file (str): chemin fichier Excel pour sauvegarde

    Returns:
        tuple: (df_all, df_L, df_F, df_autres)
    """
    logger.info("Début du Contrôle 1")

    query = f"""
    SELECT IDINTERNO, NUMFACTURA, ESTADO, FECFACTURA
    FROM LQ_FACTURA_B
    WHERE ESTADO IN ('R','A','C','N','F','G','H','K','L','E','J','V')
    AND FECFACTURA LIKE '%/{month}/{year}'
    AND BFUSIC IS NULL
    AND IDINTERNO NOT IN (SELECT IDFACTURA FROM FCABFAC)
    ORDER BY IDINTERNO
    """
    df_all = run_query(conn, query)

    df_L = df_all[df_all['ESTADO'] == 'L']
    df_F = df_all[df_all['ESTADO'] == 'F']
    df_autres = df_all[(df_all['ESTADO'] != 'L') & (df_all['ESTADO'] != 'F')]

    if not df_all.empty:
        save_to_excel(df_all, "Contrôle1_Toutes_Manquantes", output_file)
    if not df_L.empty:
        save_to_excel(df_L, "Contrôle1_ETAT_L", output_file)
    if not df_F.empty:
        save_to_excel(df_F, "Contrôle1_ETAT_F", output_file)
    if not df_autres.empty:
        save_to_excel(df_autres, "Contrôle1_Autres_Etats", output_file)

    logger.info(f"Contrôle 1 terminé : {len(df_all)} factures manquantes.")
    return df_all, df_L, df_F, df_autres
