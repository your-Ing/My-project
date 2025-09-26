import logging
from utils import run_query, save_to_excel

logger = logging.getLogger("PGOP_JDE_Control")

def controle_4(conn, year, month, output_file):
    """
    Contrôle 4: Vérifie les factures non transmises avec CODE 4.

    Args:
        conn: connexion à la base de données
        year (str): année AAAA
        month (str): mois MM
        output_file (str): chemin fichier Excel pour sauvegarde

    Returns:
        DataFrame: factures avec code 4
    """
    logger.info("Début du Contrôle 4")

    query = f"""
    SELECT PGCCID, PGASID, PGLOT, PGBP01, PG74UAMT1, PGEV01
    FROM F58PGOP1
    WHERE PGLOT LIKE '%{month}/%/{year}%'
    AND PGCCID NOT IN (SELECT RPDOC FROM F03B11)
    AND PGEV01 = 4
    ORDER BY PGCCID
    """
    df_code4 = run_query(conn, query)

    if not df_code4.empty:
        save_to_excel(df_code4, "Contrôle4_Code4", output_file)

        # Récupération des IDs pour requête client
        ids = df_code4['PGCCID'].tolist()
        if ids:
            query_client = f"""
            SELECT IDINTERNO, NUMFACTURA, NOMUSU
            FROM LQ_FACTURA_B
            WHERE IDINTERNO IN ({','.join(['%s'] * len(ids))})
            ORDER BY NOMUSU
            """
            df_clients = run_query(conn, query_client, params=ids)
            if not df_clients.empty:
                save_to_excel(df_clients, "Contrôle4_Clients", output_file)

        # Partie IFU
        query_ifu = """
        SELECT ABALPH, ABTAX
        FROM F0101
        WHERE ABALPH LIKE '%PUMA%'
        LIMIT 10
        """
        df_ifu = run_query(conn, query_ifu)
        if not df_ifu.empty:
            save_to_excel(df_ifu, "Contrôle4_IFU", output_file)

    logger.info(f"Contrôle 4 terminé : {len(df_code4) if df_code4 is not None else 0} factures avec code 4.")
    return df_code4 if df_code4 is not None else None
