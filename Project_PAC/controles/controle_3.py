import logging
from utils import run_query, save_to_excel

logger = logging.getLogger("PGOP_JDE_Control")

def controle_3(conn, year, month, output_file):
    """
    Contrôle 3: Vérifie les factures non transmises de F58PGOP1 dans F03B11 (comptabilité).

    Args:
        conn: connexion à la base de données
        year (str): année AAAA
        month (str): mois MM
        output_file (str): chemin fichier Excel pour sauvegarde

    Returns:
        DataFrame: factures manquantes en comptabilité
    """
    logger.info("Début du Contrôle 3")

    query = f"""
    SELECT PGCCID, PGASID, PGLOT, PGBP01, PG74UAMT1, PGEV01
    FROM F58PGOP1
    WHERE PGLOT LIKE '%{month}/%/{year}%'
    AND PGCCID NOT IN (SELECT RPDOC FROM F03B11)
    ORDER BY PGCCID
    """
    df_manquantes = run_query(conn, query)

    if not df_manquantes.empty:
        save_to_excel(df_manquantes, "Contrôle3_Manquantes", output_file)

    logger.info(f"Contrôle 3 terminé : {len(df_manquantes)} factures manquantes en comptabilité.")
    return df_manquantes
