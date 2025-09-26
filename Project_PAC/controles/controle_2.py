import logging
from utils import run_query, save_to_excel

logger = logging.getLogger("PGOP_JDE_Control")

def controle_2(conn, year, month, output_file):
    """
    Contrôle 2: Vérifie les factures non-transmises de FCABFAC dans F58PGOP1.

    Args:
        conn: connexion à la base de données
        year (str): année AAAA
        month (str): mois MM
        output_file (str): chemin fichier Excel pour sauvegarde

    Returns:
        DataFrame: factures manquantes
    """
    logger.info("Début du Contrôle 2")

    query_pgop = f"""
    SELECT k.IDFACTURA, k.NUMFACTURA, k.CABDSP, k.FECFACTURA, k.TIPOSERIE, k.IMPNET, l.ESTADO
    FROM FCABFAC k
    INNER JOIN LQ_FACTURA_B l ON k.IDFACTURA = l.IDINTERNO
    WHERE l.FECFACTURA LIKE '%/{month}/{year}'
    AND l.BFUSIC IS NULL
    AND k.CABDSP = 'Y'
    ORDER BY k.NUMFACTURA
    """
    df_pgop = run_query(conn, query_pgop)

    query_jde = f"""
    SELECT PGCCID, PGASID, PGLOT, PGBP01, PG74UAMT1/100 as Montant
    FROM F58PGOP1
    WHERE PGLOT LIKE '{month}/%/{year}%'
    ORDER BY PGASID
    """
    df_jde = run_query(conn, query_jde)

    df_manquantes = None
    if not df_pgop.empty:
        ids_jde = set(df_jde['PGASID'].astype(int).unique()) if not df_jde.empty else set()
        df_manquantes = df_pgop[~df_pgop['IDFACTURA'].astype(int).isin(ids_jde)]

        if not df_manquantes.empty:
            save_to_excel(df_manquantes, "Contrôle2_Manquantes", output_file)

    logger.info(f"Contrôle 2 terminé : {len(df_manquantes) if df_manquantes is not None else 0} factures manquantes.")
    return df_manquantes if df_manquantes is not None else None
