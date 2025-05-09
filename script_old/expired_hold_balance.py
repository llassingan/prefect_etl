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
config = appserver.ActiveConfig
app    = config.AppObject
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
  batch_sql.updateResources(SQL_RESOURCES)
  
  main(config)
  returns.CreateValues(['Is_Err', 0])

  return 1

def main(config):
  helper = phelper.PObjectHelper(config)
  oToday = helper.CreateObject('PeriodHelper').GetToday()
  oNextDay = oToday.NextWorkDay()
    
  if not _CONSOLE:  
    global app
    app.ConCreate('out')
  #--
                                
  printOut('Processing expired hold balance on %s...' % oToday.GetDateText())   

  dictParam = {'NextDay': sutil.toDate(config, oNextDay.GetDate())}
  dictParam.update(dbutil.mapDBTableNames(config, 
    [
      'RekeningLiabilitas'
      , 'HoldDanaRekening'
      , 'NotaDebetInternal'
    ])
  )

  SQLFlows = [
    'HB_ProcessBalance', 'HB_UpdateStatus', 'CHQ_UpdateStatus'
  ]
  batch_sql.executeFlow(config, SQLFlows, dictParam)
  
  printOut('Processing expired hold balance done!')
#-- main

#-- SQL RESOURCES
#-  expired hold balance process

SQL_RESOURCES = {
  'HB_ProcessBalance': '''
    UPDATE {RekeningLiabilitas} RL SET Saldo_Ditahan = (
      SELECT 
        CASE WHEN Nominal_Hold < RL.Saldo_Ditahan 
          THEN RL.Saldo_Ditahan - Nominal_Hold
          ELSE 0.0
        END
      FROM (
        SELECT Nomor_Rekening, SUM(Nominal_Hold) as Nominal_Hold
        FROM {HoldDanaRekening}
        WHERE Tanggal_Kadaluarsa < {NextDay} + 1
          AND Status = 'A' 
        GROUP BY Nomor_Rekening
      ) q
      WHERE Nomor_Rekening = RL.Nomor_Rekening
    )
    WHERE EXISTS (
      SELECT 1 FROM {HoldDanaRekening}
      WHERE Nomor_Rekening = RL.Nomor_Rekening
        AND Tanggal_Kadaluarsa < {NextDay} + 1
        AND Status = 'A'
    )
  '''
  ,
  'HB_UpdateStatus': '''
    UPDATE {HoldDanaRekening} SET Status = 'N'
    WHERE Tanggal_Kadaluarsa < {NextDay} + 1
      AND Status = 'A'
  '''
  ,
  'CHQ_UpdateStatus' : '''
    UPDATE {NotaDebetInternal} SET Status_Warkat = 'B'
    WHERE Expired_Date < {NextDay} + 1
      AND Status_Warkat = 'A'
  '''
}