import sys
import com.ihsan.util.dbutil as dbutil
import com.ihsan.foundation.pobjecthelper as phelper
import com.ihsan.util.modman as modman
import com.ihsan.foundation.appserver as appserver

# application-level modules, loaded via modman
modman.loadStdModules(globals(), 
  [
    "sutil"
    , "AppError"
    , "scripts#batchprocess.batch_sql"
  ]
)

sutil.setDialectToORACLE()

# GLOBALS
config  = appserver.ActiveConfig
app     = config.AppObject
_CONSOLE = False

def printOut(sOut):
  global app
  
  if _CONSOLE:
    print sOut
  else:
    app.ConWriteln(sOut)
  #--
    
def DAFScriptMain(config, parameter, returns):
  # config: ISysConfig object
  # parameter: TPClassUIDataPacket
  # returnpacket: TPClassUIDataPacket (undefined structure)
  
  global _CONSOLE
  
  _CONSOLE = False
  batch_sql._CONSOLE = False
  batch_sql.app      = app
  #batch_sql.updateResources(SQL_RESOURCES)
  
  main(config)
  returns.CreateValues(['Is_Err', 0])

  return 1

def main(config):
  helper = phelper.PObjectHelper(config)
  periodHelper = helper.CreateObject('PeriodHelper')

  oToday = periodHelper.GetToday()
  oNextDay = oToday.NextWorkDay()
    
  app = config.AppObject
  app.ConCreate('out')

  #####--- 1. UPDATE NEW ACCOUNT STATUS ---- #######
  
  printOut('>> Update new account status')
  params = {
    'RekeningLiabilitas': config.MapDBTableName('RekeningLiabilitas')
    , 'NextDay': sutil.toDate(config, oNextDay.GetDate())
  }
  dbutil.runSQL(config, '''
  	UPDATE {RekeningLiabilitas} 
  	SET Is_Rekening_Baru = 'F' 
  	WHERE 
  		Is_Rekening_Baru = 'T' 
      AND Tanggal_Buka <= {NextDay} - 30 
  '''.format(**params))     			
      
  #####--- 2. UPDATE UNAUTHORIZED DEPOSIT ACCOUNT ---- #######
  printOut('>>> Update unprocess time deposit')
  params = {
    'RekeningTransaksi': config.MapDBTableName('RekeningTransaksi')
    , 'RekeningLiabilitas' : config.MapDBTableName('RekeningLiabilitas')
    , 'Deposito': config.MapDBTableName('Deposito') 
  }
  dbutil.runSQL(config, '''
    UPDATE {RekeningTransaksi} a 
    SET status_rekening = 3 
    WHERE status_rekening = 1 AND EXISTS ( 
  	  SELECT 1 FROM {Deposito}
      WHERE nomor_rekening = a.nomor_rekening 
  	    AND is_sudah_disetor = 'F'
    ) and saldo = 0.0    
  '''.format(**params))
  
  dbutil.runSQL(config, '''
    UPDATE  {RekeningLiabilitas} a 
    SET tanggal_tutup = tanggal_buka 
    WHERE 
      EXISTS (
        select 1 from {RekeningTransaksi}
        where status_rekening = 3 
           and nomor_rekening = a.nomor_rekening
      )
      AND 
      EXISTS ( 
  	  SELECT 1 FROM {Deposito}
      WHERE nomor_rekening = a.nomor_rekening 
  	    AND is_sudah_disetor = 'F'
      )
      and tanggal_tutup is null
  '''.format(**params))
  
  #####--- 3. RESET COUNTER REKENING ---- #######
  
  printOut('>>> Reset counter rekening')
  params = {'CounterTransaksiRekening': config.MapDBTableName('CounterTransaksiRekening')}    
  dbutil.runSQL(config, '''
    UPDATE {CounterTransaksiRekening} 
    SET 
        Jumlah_Transaksi = 0, 
        Nilai_Transaksi = 0.0 
    WHERE 
        Periode = 'H' 
  '''.format(**params))
  
  #####--- 4. COPY KODE POF NULL ---- #######
  
  printOut('>>> Update kode pof rekening null')
  params = {'RekeningLiabilitas': config.MapDBTableName('RekeningLiabilitas')
    , 'Produk': config.MapDBTableName('Produk')
  }
  dbutil.runSQL(config, '''
    UPDATE {RekeningLiabilitas} rl
    SET 
        kode_pof = coalesce(p.kode_pof_default, '000')
    FROM {Produk} p  
    WHERE 
        rl.kode_produk = p.kode_produk
        and rl.kode_pof is null
  '''.format(**params))
  
  app.ConWriteln('>>> Process selesai')
  
  #####--- 5. UPDATE ACCOUNT ---- #######
  printOut('>>> Update unprocess time deposit')
  params = {
    'RekeningTransaksi': config.MapDBTableName('RekeningTransaksi')
    , 'RekeningLiabilitas' : config.MapDBTableName('RekeningLiabilitas')
    , 'Deposito': config.MapDBTableName('Deposito') 
  }
  dbutil.runSQL(config, '''
    UPDATE {RekeningTransaksi} a 
    SET status_rekening = 3 
    WHERE status_rekening = 1 AND EXISTS ( 
  	  SELECT 1 FROM {Deposito}
      WHERE nomor_rekening = a.nomor_rekening 
  	    AND is_sudah_disetor = 'F'
    ) and saldo = 0.0    
  '''.format(**params))
  
  return 1