import os
import logging
import argparse
from datetime import datetime
from utils import load_config, get_db_connection, send_email, save_to_excel
from controles.controle_1 import controle_1
from controles.controle_2 import controle_2
from controles.controle_3 import controle_3
from controles.controle_4 import controle_4
from controles.controle_5 import controle_5
import pandas as pd

# -----------------------
# CONFIGURATION LOGGING
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"execution_{datetime.now().strftime('%Y%m%d_%H%M')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("PGOP_JDE_Control")

# -----------------------
# MAIN
# -----------------------
def main():
    parser = argparse.ArgumentParser(description="Script de contrôle factures PGOP → JDE")
    parser.add_argument("-am", type=str, required=True, help="Format AAAAMM, ex: 202407")
    args = parser.parse_args()

    year = args.am[:4]
    month = args.am[4:6]

    config = load_config()

    OUTPUT_DIR = "output"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    # Fichiers Excel spécifiques
    rapport_file = os.path.join(OUTPUT_DIR, f"rapport_controles_{timestamp}.xlsx")

    try:
        conn = get_db_connection()

        # -------- CONTROL 1 --------
        df_all, df_L, df_F, df_autres = controle_1(conn, year, month, rapport_file)

        # -------- CONTROL 2 --------
        df_c2 = controle_2(conn, year, month, rapport_file)

        # -------- CONTROL 3 --------
        df_c3 = controle_3(conn, year, month, rapport_file)

        # -------- CONTROL 4 --------
        df_c4 = controle_4(conn, year, month, rapport_file)

        # -------- CONTROL 5 --------
        df_c5 = controle_5(conn, year, month, rapport_file)

        conn.close()
        logger.info("Tous les contrôles terminés.")

        # -----------------------
        # ENVOI EMAILS CIBLES
        # -----------------------

        # Destinataires depuis config
        dest1 = config['email']['dest1'].split(',')
        dest2 = config['email']['dest2'].split(',')
        dest3 = config['email']['dest3'].split(',')
        dest4 = config['email']['dest4'].split(',')

        # Email destinataire 1(Admin_PGOP) : ("Autres états" contrôle 1à et controle 2
        ids_factures = ','.join(map(str, df_autres['IDINTERNO'].tolist()))
        body1 = "<p>Bonjour Hermione,</p>"
        if df_autres.empty:
            body1 +="<p>Aucune facture 'Autres états' trouvé.</p>"
        else:
            body1 +="<p>Je te prie de bien vouloir mettre les factures suivantes à l'état 'F' dans PGOP :</p>"
            body1 +="<p> UPDATE LQ_FACTURA_B SET ESTADO = 'F' WHERE IDINTERNO IN (" + ids_factures + ");</p>"
            body1 += df_autres.to_html(index=False)
            body1 +="<p>Merci, <br> Cordialement </p>"
        send_email(f"[PGOP] Contrôle 1 - Autres états {year}/{month}", body1, recipients=dest1)
        
        # Email destination 1(Admin_PGOP) : (Control 2) Facture transmise ou non de PGOP vers JDE
        if df_c2.empty:
            body5 = "<p> Aucun décalage entre PGOP et JDE </p>"
        else:
            body5 = "<p> Bien vouloir effectuer le transfert des factures suivantes de PGOP vers JDE </p>"
            body5 += df_c2.to_html(index=False)   
            body5 += "<p> Merci, <br> Cordialement. </p>"   
        send_email(f"[PGOP-JDE] Contrôle 2  {year}/{month}", body5, recipients=dest1)    
    

        # Email destinataire 2 (facturation) : Factures L et F contrôle 1
        if df_L.empty:
            body2 = "<p>Aucune facture 'L' en attente de transfert.</p>"
        else:
            body2 = "<p>Je te prie de bien vouloir traiter les factures à l'état 'L' dans PGOP, en attente de transfert:</p>"
            body2 += df_L.to_html(index=False)
            
        if df_F.empty:
            body2 += "<p>Aucune facture 'F' en attente de transfert.</p>"
        else:
            body2 += "<h3>Je te prie de bien vouloir traiter les factures à l'état 'F' dans PGOP, en attente de transfert:</h3>"
            body2 += df_F.to_html(index=False)                                    
        send_email(f"[PGOP] Contrôle 1 - États L et F {year}/{month}", body2, recipients=dest2)

        # Email destinataire 3 (Admin_JDE): Résultat contrôle 3
        if df_c3.empty:
            body3 = "<p>Aucune facture non transmise en comptabilité.</p>"
        else:
            body3 = "<h3>Contrôle 3 - Factures non transmises en comptabilité</h3>"
            body3 += df_c3.to_html(index=False)
        send_email(f"[JDE] Contrôle 3 {year}/{month}", body3, recipients=dest3)

        # Email destinataire 4 (DSI): Bilan complet avec fichier Excel
        body4 = "<h3>Bilan complet des contrôles de factures PGOP-JDE</h3>"
        body4 += "<p>Veuillez trouver ci-joint le fichier Excel contenant tous les résultats.</p>"
        send_email(f"[PGOP-JDE] Bilan complet {year}/{month}", body4, attachment_path=rapport_file, recipients=dest4)

    except Exception as e:
        logger.error(f"Erreur critique: {e}")

if __name__ == "__main__":
    main()
