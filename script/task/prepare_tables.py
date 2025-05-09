from prefect import task
from prefect.logging import get_run_logger
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Connection
from sqlalchemy import text
import sys
import os

# Add the utils directory to the path so we can import custom_log_handler
sys.path.append("/opt/prefect/script")
from ..db.database_conn import get_connection_postgres
from ..utils.config import settings 


def resetTmpUploadTrx(conn, log):
  log.info(f"Reset tmp upload trx") 
  params= {"TABLE" : ["tmp.uploadtrxitem", "tmp.uploadtrxitem_balance","tmp.uploadtrxitem_transaksi"]}
  try:
    with conn:
        with conn.cursor() as cur:
            for tableName in params['TABLE']:
              log.info(f"Truncate table {tableName}") 
              cur.execute(text(f"""TRUNCATE TABLE {tableName}"""))
    log.info("trasanction committed")
  except SQLAlchemyError as e:
    log.error(f"Database error occurred: {e}. Rolled back")

  except Exception as e:
    log.error(f"Unexpected error: {e}. Rolled back")
  
 
def createTmpDorman(conn, log):
  log.info(f"Create tmp.dorman_candidate")
  params= {"TABLE" : "tmp.dorman_candidate"}
  try:
    with conn:
        with conn.cursor() as cur:
            log.info(f"Drop table {params['TABLE']}") 
            cur.execute(text(f"""DROP TABLE {params['TABLE']}"""))
            log.info(f"Create table {params['TABLE']}") 
            cur.execute(text(f"""
              create table {params['TABLE']} 
              ( nomor_rekening VARCHAR(20) primary key
                , Saldo numeric(36,10)
                , Tgl_Trans_Cabang_Terakhir timestamp(6)
                , Tgl_Trans_Echannel_Terakhir timestamp(6)
                , Tgl_Trans_Terakhir timestamp(6)  
                , Tanggal_Buka timestamp(6)
                , Tanggal_Acuan_Dormant varchar(1)
                , Kode_Produk varchar(10)
                , Jumlah_Hari_Tidak_Aktif integer
                , param_Hari_Tidak_Aktif integer
                , saldo_minimum numeric(36,2)
                , Saldo_Minimum_Tidak_Aktif numeric(36,2)
                , status_rekening integer
                , status_rekening_update integer
                , Process_Status integer
              )
            """))
            log.info(f"Truncate table {params['TABLE']}") 
            cur.execute(text(f"""TRUNCATE TABLE {params['TABLE']}"""))
    log.info("trasanction committed")
  except SQLAlchemyError as e:
    log.error(f"Database error occurred: {e}. Rolled back")

  except Exception as e:
    log.error(f"Unexpected error: {e}. Rolled back")

def createTmpTransfer(conn, log):
  log.info(f"Create tmp.transfer_candidate")  
  params = {
    "TABLE": "tmp.transfer_candidate",
    "SEQUENCE": "tmp.seq_remittance_ref"
  }
  #try:
  #  batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  #except:
  #  pass  
  #batch_sql.runSQL(config, '''
  #  create table {TABLE} (
  #    Id_Transfer INTEGER NOT NULL
  #    , Jenis_Transfer VARCHAR(1)
  #    , Nama_Penerima VARCHAR(50)
  #    , Nomor_Rekening_Penerima VARCHAR(20)
  #    , Nama_Pengirim VARCHAR(50)
  #    , Nomor_Rekening_Pengirim VARCHAR(20)
  #    , Kode_Bank VARCHAR(20)
  #    , Id_Detil_Transaksi INTEGER
  #    , Id_Transaksi INTEGER
  #
  #    kode_account_titipan    VARCHAR(20)  NULL,
  #    kode_cabang_titipan     VARCHAR(10)  NULL,
  #    keterangan_transfer     VARCHAR(100) NULL,
  #    nominal_transfer        numeric(36,10) NULL,
  #    status                  VARCHAR(1)   NULL,
  #    keterangan_proses       VARCHAR(100) NULL,
  #    is_kena_biaya           VARCHAR(100) NULL,
  #    nominal_biaya           numeric(36,10) NULL,
  #    rekening_biaya          VARCHAR(30)  NULL,
  #    primary key (Id_Transfer)
  #  )
  # '''.format(**params)
  #)
  # replace drop and recreate by below scripts
  try:
    with conn:
        with conn.cursor() as cur:
            log.info(f"Truncate table {params['TABLE']}") 
            cur.execute(text(f"""TRUNCATE TABLE {params['TABLE']}"""))
            log.info(f"drop sequence {params['SEQUENCE']}") 
            cur.execute(text(f"""DROP SEQUENCE {params['SEQUENCE']}"""))
            log.info(f"create sequence {params['SEQUENCE']}") 
            cur.execute(text(f"""CREATE SEQUENCE {params['SEQUENCE']}"""))
    log.info("trasanction committed")
  except SQLAlchemyError as e:
    log.error(f"Database error occurred: {e}. Rolled back")

@task
def PrepareTable():
  conn: Connection = get_connection_postgres()
  logger = get_run_logger()
  logger.info("Starting task")
  try:
    resetTmpUploadTrx(conn,logger)
    createTmpDorman(conn,logger)
    createTmpTransfer(conn,logger)
  finally:
    conn.close()
    logger.info("Connection closed")


  

def main(config):
  global app
  
  batch_sql._CONSOLE = False
  batch_sql.app      = app
  
  batch_sql.printOut('Preparing core batch process temporary tables')
  createTmpTransaction(config)
  createTmpTransfer(config)
  createStandingInstruction(config)
  createProfitDistribution(config)
  #createTmpDeletedTransaction(config)
  createDPK(config)
  createSavingProfitDistribution(config)
  createSavingAverageBalance(config)
  createAdmCost(config)
  createGLTransfer(config)
  createBHRAK(config)
  create_timedeposit_revaccrual(config)
  createPremiAsuransi(config)
  createTmpDorman(config)
  createTmpDepreciable(config)
  batch_sql.printOut('Creating temporary tables done')

  resetTmpUploadTrx(config)
  

          
def createTmpTransaction(config):
  batch_sql.printOut('Create tmp.detil_candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.detil_candidate')
    , 'SEQUENCE': config.MapDBTableName('tmp.seq_detil_candidate')
  }
  #try:
  #  batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  #except:
  #  pass
  #batch_sql.runSQL(config, '''
  #  create table {TABLE} (
  #    id_temp integer not null,
  #    id_transaksi integer,
  #    id_detil_transaksi integer,
  #    nomor_rekening varchar(20) not null,
  #    kode_cabang varchar(10),
  #    kode_valuta varchar(3),
  #    kode_tx_class varchar(10),
  #    mnemonic varchar(1),
  #    amount numeric(38, 16),
  #    kode_kurs varchar(20),
  #    nilai_kurs numeric(38, 16),
  #    description varchar(100),
  #    kode_jurnal varchar(10),
  #    jenis_detil_transaksi varchar(1),
  #    kode_account varchar(20),      
  #    
  #    
  #    primary key (id_temp)
  #  )
  #  '''.format(**params)
  #)
  # replace drop and recreate by below scripts
  batch_sql.runSQL(config, '''TRUNCATE TABLE {TABLE}'''.format(**params))
  try:
    batch_sql.runSQL(config, '''DROP SEQUENCE {SEQUENCE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, ''' create sequence {SEQUENCE} '''.format(**params))

  batch_sql.printOut('Create tmp.tran_candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.tran_candidate')
    , 'SEQUENCE': config.MapDBTableName('tmp.seq_tran_candidate')
  }
  #try:
  # batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  #except:
  #  pass
  #batch_sql.runSQL(config, '''
  #  create table {TABLE} (
  #    id_temp integer not null,
  #    id_transaksi integer,
  #    kode_cabang varchar(10),
  #    kode_kantor varchar(10),
  #    nomor_referensi varchar(50),
  #    jenis_aplikasi VARCHAR(1),
  #    description varchar(100),
  #    kode_transaksi varchar(20),
  #    session_id integer,
  #    primary key (id_temp)
  #  )
  #  '''.format(**params)
  #)
  # replace drop and recreate by below scripts
  batch_sql.runSQL(config, '''TRUNCATE TABLE {TABLE}'''.format(**params))
  try:
    batch_sql.runSQL(config, '''DROP SEQUENCE {SEQUENCE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, ''' create sequence {SEQUENCE} '''.format(**params))

  batch_sql.printOut('Create tmp.detiltransaksi_candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.detiltransaksi_candidate')
    , 'SEQUENCE': config.MapDBTableName('tmp.seq_detiltransaksi_candidate')
  }
  #try:
  #  batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  #except:
  #  pass
  #batch_sql.runSQL(config, '''
  #  create table {TABLE} (
  #   id_temp int4 NOT NULL,
  #   id_transaksi int4 NULL,
  #   id_detil_transaksi int4 NULL,
  #   transession_id int4 NULL,
  #   nomor_rekening varchar(20) NOT NULL,
  #   kode_cabang varchar(5) NULL,
  #   kode_valuta varchar(3) NULL,
  #   kode_tx_class varchar(10) NULL,
  #   mnemonic varchar(1) NULL,
  #   amount numeric(38, 16) NULL,
  #   kode_kurs varchar(20) NULL,
  #   nilai_kurs numeric(38, 16) NULL,
  #   description varchar(200) NULL,
  #   kode_jurnal varchar(10) NULL,
  #   jenis_detil_transaksi varchar(1) NULL,
  #   kode_account varchar(20) NULL,
  #   id_parameter_transaksi varchar(5) NULL,
  #   PRIMARY KEY (id_temp)  
  #  )
  #  '''.format(**params)
  #)
  # replace drop and recreate by below scripts
  batch_sql.runSQL(config, '''TRUNCATE TABLE {TABLE}'''.format(**params))
  try:
    batch_sql.runSQL(config, '''DROP SEQUENCE {SEQUENCE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, ''' create sequence {SEQUENCE} '''.format(**params))

  batch_sql.printOut('Create tmp.transaksi_candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.transaksi_candidate')
    , 'SEQUENCE': config.MapDBTableName('tmp.seq_transaksi_candidate')
  }

  
  #try:
  # batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  #except:
  #  pass
  #batch_sql.runSQL(config, '''
  #  create table {TABLE} (
  #  id_temp int4 NOT NULL,
  #  id_transaksi int4 NULL,
  #  transession_id int4 NULL,
  #  kode_cabang varchar(5) NULL,
  #  description varchar(200) NULL,
  #  kode_transaksi varchar(20) NULL,
  #  nomor_referensi varchar(50) NULL,
  #  journal_no varchar(30) NULL,
  #  is_confidential varchar(1) NULL,
  #  id_confidential int4 NULL,
  #  tanggal_input timestamp NULL,
  #  jam_input timestamp NULL,
  #  PRIMARY KEY (id_temp) 
  #  )
  #  '''.format(**params)
  #)
  # replace drop and recreate by below scripts
  batch_sql.runSQL(config, '''TRUNCATE TABLE {TABLE}'''.format(**params))
  try:
    batch_sql.runSQL(config, '''DROP SEQUENCE {SEQUENCE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, ''' create sequence {SEQUENCE} '''.format(**params))

  batch_sql.printOut('Create tx_balance tables')
  params = {
    'TABLE': config.MapDBTableName('tmp.tx_balance')
    , 'SEQUENCE': config.MapDBTableName('tmp.seq_tx_balance')
    , 'INDEX': config.MapDBTableName('tmp.idx_tx_balance')
    , 'TX_ACCOUNTBALANCE' : config.MapDBTableName('tmp.tx_accountbalance')
  }
  #try:
  #  batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  #except:
  #  pass
  #batch_sql.runSQL(config, '''
  #  create table {TABLE} (
  #    id_balance integer not null,
  #    table_name varchar(32) not null,
  #    field_name varchar(32) not null,
  #    nomor_rekening varchar(20) not null,
  #    total_debit numeric(38, 16),
  #    total_credit numeric(38, 16),
  #    balance numeric(38, 16),
  #    kode_tx varchar(20),
  #    
  #    primary key (id_balance)
  #  )
  #  '''.format(**params)
  #)
  # replace drop and recreate by below scripts
  batch_sql.runSQL(config, '''TRUNCATE TABLE {TABLE}'''.format(**params))
  batch_sql.runSQL(config, '''TRUNCATE TABLE {TX_ACCOUNTBALANCE}'''.format(**params))
  
  try:
    batch_sql.runSQL(config, '''DROP SEQUENCE {SEQUENCE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, ''' create sequence {SEQUENCE} '''.format(**params))
  try:
    batch_sql.runSQL(config, '''drop index {INDEX}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, ''' 
    create unique index idx_tx_balance 
    on {TABLE}(nomor_rekening, table_name, field_name, kode_tx)
  '''.format(**params))

def createStandingInstruction(config):
  batch_sql.printOut('Create balanceaccum tables')
  params = {'TABLE': config.MapDBTableName('tmp.ltmp_balanceaccum')}
  #try:
  #  batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  #except:
  #  pass
  #batch_sql.runSQL(config, '''
  #  create table {TABLE} (
  #    nomor_rekening varchar(20) not null,
  #    total_debit numeric(38, 16),
  #    balance numeric(38, 16),
  #    
  #    primary key (nomor_rekening)
  #  )
  #  '''.format(**params)
  #)
  # replace drop and recreate by below scripts
  batch_sql.runSQL(config, '''TRUNCATE TABLE {TABLE}'''.format(**params))

def createProfitDistribution(config):
  batch_sql.printOut('Create profit account tables')
  params = {
    'TABLE': config.MapDBTableName('tmp.profit_account')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20) not null,
      kode_valuta varchar(10),
      jenis_akad varchar(1),
      nisbah numeric(38,16),
      saldo numeric(38,16),
      is_tiering_profit varchar(1),
      id_tiering integer,
      day_number integer,
      duration integer,
      amount numeric(38,16),
      kode_pof varchar(10),
      amount_reguler numeric(38,16),
      amount_target numeric(38,16),
      amount_smoothing numeric(38,16),
      is_smoothing_gdr varchar(1),
      is_bagihasil_smoothing varchar(1),
      gdrval numeric(38,16),
      nisbah_dasar numeric(38,16),
      nisbah_spesial numeric(38,16),
      persentase_zakat numeric(10,4),
      tarif_pajak numeric(10,4),
      ekuivalen_rate numeric(10,4),
      amount_rate numeric(36,10),
      primary key (nomor_rekening)
    )
    '''.format(**params)
  )
  # replace drop and recreate by below scripts
  #batch_sql.runSQL(config, '''TRUNCATE TABLE {TABLE}'''.format(**params))

  batch_sql.printOut('Create profit tiering tables')
  params = {
    'TABLE': config.MapDBTableName('tmp.profit_tiering')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20) not null,
      nisbah numeric(38,16),
      saldo numeric(38,16),
      day_number integer,
      duration integer,
      jenis_akad varchar(1),
      kode_valuta varchar(10),
      amount numeric(38,16),
      nisbah_dasar numeric(38,16),
      nisbah_spesial numeric(38,16)
        
    )
    '''.format(**params)
  )
  # replace drop and recreate by below scripts
  #batch_sql.runSQL(config, '''TRUNCATE TABLE {TABLE}'''.format(**params))

  batch_sql.printOut('Create timedeposit profit distribution candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.profdist_candidate')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20),
      nominal numeric(38,16),
      disposisi varchar(1),
      nomor_disposisi varchar(20),
      total_saldo numeric(38,16),
      kode_cabang varchar(10),
      rekening_zakat varchar(20),
      pajak numeric(38,16),
      zakat numeric(38,16),
      id_transaksi integer,
      status integer,
      kode_produk varchar(10),
      is_kena_pajak varchar(1),
      tarif_pajak numeric(38,16),
      is_kena_zakat varchar(1),
      tarif_zakat numeric(38,16),
      jenis_penduduk varchar(1),
      procstatus varchar(100),
      nominal_bagihasil numeric(38,16),
      nominal_rate numeric(38,16),
      saldo numeric(38, 16),
      day_number integer,
      id_transaksi_transfer integer,
      is_cad_bagihasil_net varchar(1),
      is_hitung_baghas_jt varchar(1),
      status_skb varchar(1),
      nomor_surat_skb varchar(100),
      tgl_berakhir_skb date,
      nomor_nasabah varchar(20),
      periode_awal timestamp,
      periode_akhir timestamp,
      primary key (nomor_rekening)
    )
    '''.format(**params)
  )

  batch_sql.printOut('Create timedeposit maturity process candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.maturity_candidate')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20),
      nominal numeric(38,16),
      disposisi varchar(1),
      nomor_disposisi varchar(20),
      total_saldo numeric(38,16),
      kode_valuta varchar(10),
      kode_cabang varchar(10),
      id_transaksi integer,
      status integer,
      kode_produk varchar(10),
      tanggal_buka timestamp,
      is_aro varchar(1),
      is_blokir varchar(1),
      jumlah_aro integer,
      cadangan_baghas numeric(38,16),
      rekening_zakat varchar(20),
      pajak numeric(38,16),
      zakat numeric(38,16),
      is_kena_pajak varchar(1),
      tarif_pajak numeric(38,16),
      is_kena_zakat varchar(1),
      tarif_zakat numeric(38,16),
      jenis_penduduk varchar(1),
      procstatus varchar(100),
      periode_awal timestamp,
      periode_akhir timestamp,
      primary key (nomor_rekening)
    )
    '''.format(**params)
  )

  batch_sql.printOut('Create timedeposit accrual candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.timedeposit_accrual')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20),
      saldo numeric(38,16),
      nominal numeric(38,16),
      nominal_net numeric(38,16),
      nominal_pajak numeric(38,16),
      nominal_zakat numeric(38,16), 
      id_transaksi numeric(38,0),
      id_reverse numeric(38,0),
      rekening_pajak varchar(20),
      rekening_zakat varchar(20),
      kode_cabang varchar(10),
      kode_valuta varchar(10),
      kode_produk varchar(10),
      primary key (nomor_rekening)
    )
    '''.format(**params)
  )
  
  
  batch_sql.printOut('Create timedeposit accrual smoothing candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.timedeposit_accrual_smoothing')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
    
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20),
      nominal numeric(38,16),
      nominal_reguler numeric(38,16),
      nominal_target numeric(38,16),
      id_transaksi numeric(38,0),
      id_transaksi_rev numeric(38,0),
      kode_cabang varchar(10),
      kode_valuta varchar(10),
      kode_produk varchar(10),
      account_code_smoothing varchar(20),
      rekening_smoothing varchar(30),
      smoothing_type varchar(1),
      kode_pof varchar(10),
      gdrval_reguler numeric(38,16),
      gdrval_target numeric(38,16),
      primary key (nomor_rekening)
    )
    '''.format(**params)
  )

def createTmpDeletedTransaction(config):
  tables = ['TransaksiLiabilitas', 'DetilTransaksi', 'Transaksi']
                               
  for table in tables:
    batch_sql.printOut('Create deleted ' + table)
    params = {
      'TABLE': config.MapDBTableName(table)
      , 'DELETED_TABLE': config.MapDBTableName('tmp.Deleted_'+table)
    }
    try:
      batch_sql.runSQL(config, '''
        create table {DELETED_TABLE} as select * from {TABLE} where 1=0
      '''.format(**params))
    except:
      pass
    #--
  #--

def createDPK(config):
  batch_sql.printOut('Create TotalDPKNasabah')
  params = {
    'TABLE': config.MapDBTableName('totaldpknasabah')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params) )
  except:
    #raise Exception, str(sys.exc_info()[1])
    pass
  
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_nasabah varchar(20) not null,
      saldo numeric(38,16),
      primary key (nomor_nasabah)
    ) 
  '''.format(**params))

  #--

def createSavingProfitDistribution(config):
  batch_sql.printOut('Create saving profit account tables')
  params = {
    'TABLE': config.MapDBTableName('tmp.profit_saving_account')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params) )
  except:
    pass                        
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20) not null,
      kode_valuta varchar(10),
      jenis_akad varchar(1),
      nisbah numeric(38,16),
      saldo numeric(38,16),
      is_tiering_profit varchar(1),
      id_tiering integer,
      day_number integer,
      amount numeric(38,16),
      amount_reguler numeric(38,16),
      amount_smoothing numeric(38,16),
      amount_smoothing_diff numeric(38,16),
      is_amount_smoothing varchar(1),
      gdrval numeric(38,16),
      nisbah_dasar numeric(38,16),
      nisbah_spesial numeric(38,16),      
      primary key (nomor_rekening)
    )
    '''.format(**params)
  )  

  batch_sql.printOut('Create saving profit tiering tables')
  params = {
    'TABLE': config.MapDBTableName('tmp.profit_saving_tiering')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE IF EXISTS {TABLE} '''.format(**params))
  except:
    pass              
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20) not null,
      nisbah numeric(38,16),
      saldo numeric(38,16),
      day_number integer,
      jenis_akad varchar(1),
      kode_valuta varchar(10),
      amount numeric(38,16),
      nisbah_dasar numeric(38,16),
      nisbah_spesial numeric(38,16)
        
    )
    '''.format(**params)
  )

  batch_sql.printOut('Create saving profit distribution candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.saving_profdist_candidate')
    , 'INDEX11': config.MapDBTableName('tmp.idx_profdist_11')
    , 'INDEX12': config.MapDBTableName('tmp.idx_profdist_12')
    , 'INDEX13': config.MapDBTableName('tmp.idx_profdist_13')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE IF EXISTS {TABLE} '''.format(**params))
  except:
    pass             
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20),
      nominal numeric(38,16),
      rekening_biaya varchar(20),
      rekening_pajak varchar(20),
      rekening_titipan varchar(20),
      total_saldo numeric(38,16),
      saldo_awal numeric(38,16),
      kode_cabang varchar(10),
      kode_valuta varchar(10),
      rekening_zakat varchar(20),
      pajak numeric(38,16),
      zakat numeric(38,16),
      id_transaksi integer,
      status integer,
      kode_produk varchar(10),
      is_kena_pajak varchar(1),
      tarif_pajak numeric(38,16),
      is_kena_zakat varchar(1),
      tarif_zakat numeric(38,16),
      jenis_penduduk varchar(1),
      procstatus varchar(100),
      id_transaksi_zakat integer,
      id_transaksi_pajak integer,
      status_skb varchar(1),
      nomor_surat_skb varchar(100),
      tgl_berakhir_skb date,
      jenis_akad varchar(1),
      nisbah_bagi_hasil numeric(10,4),
      nomor_nasabah varchar(20),
      
      primary key (nomor_rekening)
    )
    '''.format(**params)
  )
  batch_sql.runSQL(config, ''' create index idx_profdist_11 on {TABLE} (kode_cabang) '''.format(**params))
  batch_sql.runSQL(config, ''' create index idx_profdist_12 on {TABLE} (kode_valuta) '''.format(**params))
  batch_sql.runSQL(config, ''' create index idx_profdist_13 on {TABLE} (kode_produk) '''.format(**params))

  batch_sql.printOut('Create saving profit distribution accrual candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.saving_distaccru_candidate'),
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass               
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      kode_produk varchar(10),
      kode_cabang varchar(10),
      kode_valuta varchar(10),
      rekening_biaya varchar(20),
      rekening_cadangan varchar(20),
      nominal numeric(38,16),
      id_transaksi integer,
      status integer,
      
      primary key (kode_produk, kode_cabang, kode_valuta)
    )
    '''.format(**params)
  )  
  
  batch_sql.printOut('Create saving reserve candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.saving_reserve_candidate'),
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      kode_produk varchar(10),
      kode_cabang varchar(10),
      kode_valuta varchar(10),
      rekening_biaya varchar(30),
      rekening_cadangan varchar(30),
      persentase_cadangan numeric(38,16),
      saldo numeric(38,16),
      nominal numeric(38,16),
      id_transaksi integer,
      status integer,
      
      primary key (kode_produk, kode_cabang, kode_valuta)
    )
    '''.format(**params)
  )

  batch_sql.printOut('Create gdr temporary tables')
  params = {
    'TABLE': config.MapDBTableName('tmp.gdrcalc')
    , 'SEQUENCE': config.MapDBTableName('tmp.seq_gdrcalc') 
    , 'INDEX11': config.MapDBTableName('tmp.idx_gdrcalc_11')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      id_gdr integer not null
      , kode_pof varchar(10)
      , period_code varchar(10)
      , currency_code varchar(10)
      , day_number integer
      , amount numeric(38,16)
      , amount_upperlimit numeric(38,16)
      , amount_lowerlimit numeric(38,16)
      , process_date timestamp
      , status integer
      
      , primary key (id_gdr)  
    )
    '''.format(**params)
  )
  try:
    batch_sql.runSQL(config, '''DROP SEQUENCE {SEQUENCE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, ''' create sequence {SEQUENCE} '''.format(**params))
  batch_sql.runSQL(config, ''' create index idx_gdrcalc_11 on {TABLE} (period_code) '''.format(**params))

def createPremiAsuransi(config):
  batch_sql.printOut('Create premi asuransi tables')
  params = {
    'TABLE': config.MapDBTableName('tmp.premiasuransi_candidate')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20)
      , saldo numeric(36,10)
      , id_asuransi integer
      , tanggal timestamp(6)
      , primary key (nomor_rekening, id_asuransi, tanggal)
    )
  '''.format(**params))
  
def createSavingAverageBalance(config):
  batch_sql.printOut('Create saving average balance tables')
  params = {
    'TABLE': config.MapDBTableName('tmp.saving_avg_balance')
    , 'INDEX': config.MapDBTableName('tmp.idx_avg_balance') 
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20)
      , nilai numeric(38,16)
    )
  '''.format(**params))
  batch_sql.runSQL(config, '''
    create index idx_avg_balance on {TABLE} (nomor_rekening)
  '''.format(**params))

  batch_sql.printOut('Create initial balance container tables')
  params = {
    'TABLE': config.MapDBTableName('tmp.initial_balance_container')
    , 'INDEX': config.MapDBTableName('tmp.idx_initial_balance') 
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20)
      , nilai numeric(38,16)
    )
  '''.format(**params))
  batch_sql.runSQL(config, '''
    create index idx_initial_balance on {TABLE} (nomor_rekening)
  '''.format(**params))

  batch_sql.printOut('Create mutation balance container tables')
  params = {
    'TABLE': config.MapDBTableName('tmp.mutation_balance_container')
    , 'INDEX': config.MapDBTableName('tmp.idx_mutation_balance') 
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20)
      , nilai numeric(38,16)
    )
  '''.format(**params))
  batch_sql.runSQL(config, '''
    create index idx_mutation_balance on {TABLE} (nomor_rekening)
  '''.format(**params))
  
def createAdmCost(config):
  batch_sql.printOut('Create adm cost candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.admcost_candidate')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20),
      nominal numeric(38,16),
      nominal_sisa numeric(38,16),
      total_saldo numeric(38,16),
      kode_cabang varchar(10),
      kode_valuta varchar(10),
      id_transaksi integer,
      status integer,
      kode_produk varchar(10),
      rekening_pendapatan varchar(20),
      amount_baghas numeric(38, 16),
      sumber_biaya integer,
      
      primary key (nomor_rekening)
    )
    '''.format(**params)
  )
  
  batch_sql.printOut('Create bea materai candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.bea_candidate')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20),
      nominal numeric(38,16),
      total_saldo numeric(38,16),
      kode_cabang varchar(10),
      kode_valuta varchar(10),
      id_transaksi integer,
      status integer,
      kode_produk varchar(10),
      rekening_pendapatan varchar(20),
      
      primary key (nomor_rekening)
    )
    '''.format(**params)
  )

  batch_sql.printOut('Create atm cost candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.atmcost_candidate')
  }
  
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20),
      nominal numeric(38,16),
      nominal_sisa numeric(38,16),
      total_saldo numeric(38,16),
      kode_cabang varchar(10),
      kode_valuta varchar(10),
      id_transaksi integer,
      status integer,
      kode_produk varchar(10),
      rekening_pendapatan varchar(20),
      amount_baghas numeric(38, 16),
      sumber_biaya integer,
      
      primary key (nomor_rekening)
    )
    '''.format(**params)
  )
  
  batch_sql.printOut('Create dormant cost candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.dormantcost_candidate')
  }
  
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      nomor_rekening varchar(20),
      nominal numeric(38,16),
      nominal_sisa numeric(38,16),
      total_saldo numeric(38,16),
      kode_cabang varchar(10),
      kode_valuta varchar(10),
      id_transaksi integer,
      status integer,
      kode_produk varchar(10),
      rekening_pendapatan varchar(20),
      amount_baghas numeric(38, 16),
      sumber_biaya integer,
      
      primary key (nomor_rekening)
    )
    '''.format(**params)
  )

def createGLTransfer(config):
  batch_sql.printOut('Create gl transfer candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.gl_transfer_candidate')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
    
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      rekening_sumber varchar(30),
      kode_cabang varchar(10),
      kode_valuta varchar(10),
      kode_account varchar(20),
      rekening_tujuan varchar(30),
      kode_cabang_tujuan varchar(10),
      kode_account_tujuan varchar(20),
      saldo numeric(38,16),
      balance_sign integer,
      id_transaksi integer,
      status integer,      
      primary key (rekening_sumber)
    )
  '''.format(**params)
  )

def createBHRAK(config):
  batch_sql.printOut('Create bhrak candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.bhrak_candidate')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
        id_bhrak  integer not null
      , id_transaksi integer
      , kode_cabang varchar(10)
      , jenis_mutasi varchar(1)
      , rekening_bhrak varchar(20)
      , rekening_main varchar(20)
      , nominal numeric(38,16)
      
      , primary key (id_bhrak, kode_cabang)
    )
    '''.format(**params)
  )
  #-- adjustment_RAK
  batch_sql.printOut('Create adjustment RAK candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.adjustment_rak'),
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
      adjustid integer not null,
      adjustdate timestamp,
      branch_code varchar(10),
      currency_code varchar(10),
      gl_debet varchar(20),
      gl_credit varchar(20),
      bal_aktiva numeric(38, 16),
      bal_pasiva numeric(38, 16), 
      db_account varchar(30),
      cr_account varchar(30),
      mutation numeric(38, 16),
      
      db_aktiva numeric(38, 16),
      cr_aktiva numeric(38, 16),
      db_pasiva numeric(38, 16),
      cr_pasiva numeric(38, 16),
      
      acc_aktiva varchar(20),
      acc_pasiva varchar(20),
      
      primary key (adjustid)
    )
    '''.format(**params)
  )
  
def create_timedeposit_revaccrual(config):
  batch_sql.printOut('Create timedeposit_revaccrual')
  params = {
    'TABLE': config.MapDBTableName('tmp.timedeposit_revaccrual')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE {TABLE}'''.format(**params))
  except:
    pass
  batch_sql.runSQL(config, '''
    create table {TABLE} (
        nomor_rekening varchar(20)
      , nominal numeric(38,16)
      , saldo numeric(38,16)
      , id_transaksi integer
      , id_reverse integer
      
      , primary key (nomor_rekening)
    )
    '''.format(**params)
  )

def createTmpDepreciable(config):
  batch_sql.printOut('Create depreciation candidate')
  params = {
    'TABLE': config.MapDBTableName('tmp.depreciation_candidate')
  }
  try:
    batch_sql.runSQL(config, '''DROP TABLE IF EXISTS {TABLE}'''.format(**params))
  except:
    pass
    
  batch_sql.runSQL(config, '''
    create TABLE {TABLE}(
      nomor_rekening varchar(30),
      jenis_rekening varchar(10),
      kode_cabang varchar(10),
      kode_valuta varchar(10),
      kode_produk varchar(10),
      account_code_akum varchar(30),
      account_code_biaya varchar(30),
      rekening_biaya varchar(30),
      nominal_depr numeric(38,10),
      id_transaksi integer,
      status integer,      
      primary key (nomor_rekening)
    )
  '''.format(**params)
  )