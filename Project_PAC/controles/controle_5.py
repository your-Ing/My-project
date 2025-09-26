import logging
from utils import run_query, save_to_excel
import pandas as pd

logger = logging.getLogger("PGOP_JDE_Control")

def controle_5(conn, year, month, output_file):
    """
    Contrôle 5: Réconciliation des montants entre LQ_FACTURA_B et F03B11.

    Args:
        conn: connexion à la base de données
        year (str): année AAAA
        month (str): mois MM
        output_file (str): chemin fichier Excel pour sauvegarde

    Returns:
        DataFrame: écarts de montants
    """
    logger.info("Début du Contrôle 5")

    query_pgop = f"""
    SELECT IDINTERNO, NUMFACTURA, IMPNET, IMPIVA, IMPTOT
    FROM LQ_FACTURA_B
    WHERE FECFACTURA LIKE '%/{month}/{year[-2:]}'
    AND BFUSIC IS NULL
    AND ESTADO IN ('R','A','C','N','F','G','H','K','L','E','J','V')
    ORDER BY NUMFACTURA
    """
    df_pgop = run_query(conn, query_pgop)

    query_jde = f"""
    SELECT RPDOC, RPATXA, RPSTAM, RPAG
    FROM F03B11
    WHERE RPVR01 LIKE '%|PAC|{year}%'
    ORDER BY RPDOC
    """
    df_jde = run_query(conn, query_jde)

    df_ecarts = pd.DataFrame()

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
        else:
            logger.info("Contrôle 5 OK: Aucun écart de montant.")
    else:
        logger.warning("Contrôle 5: Données insuffisantes.")

    logger.info(f"Contrôle 5 terminé : {len(df_ecarts)} écarts trouvés.")
    return df_ecarts
