SQL_RESOURCES = {
    "initSaldoAccrual": """
        update RekeningLiabilitas set saldo_accrual_bagihasil = 0.0 where saldo_accrual_bagihasil is null
    """,
    "initSaldoBaghas": """
        update RekeningLiabilitas set saldo_bagihasil = 0.0 where saldo_bagihasil is null
    """,
    "initSaldoPajak": """
        update RekeningLiabilitas set saldo_pajak = 0.0 where saldo_pajak is null
    """,
    "initSaldoZakat": """
        update RekeningLiabilitas set saldo_zakat = 0.0 where saldo_zakat is null
    """,
    "initSaldoBiaya": """
        update RekeningLiabilitas set saldo_biaya = 0.0 where saldo_biaya is null
    """,
    "initNisbahDasarRekening": """
        update RekeningLiabilitas set nisbah_dasar = 0.0 where nisbah_dasar is null
    """,
    "initNisbahSpesial": """
        update RekeningLiabilitas set nisbah_spesial = 0.0 where nisbah_spesial is null
    """,
    "initSaldoDitahan": """
        update RekeningLiabilitas set saldo_ditahan = 0.0 where saldo_ditahan is null
    """,
    "initTieringNisbah": """
        update RekeningLiabilitas set Is_Tiering_NisbahBonus = 'F' where Is_Tiering_NisbahBonus is null
    """,
    "initCadangan": """
        update Deposito set cadangan_bagihasil_kapitalisir = 0.0 where cadangan_bagihasil_kapitalisir is null
    """,
    "FixConfidential": """
        update Transaksi t
        set is_confidential = 'T'
        where exists ( select 1 from DetilTransaksi where id_transaksi = t.id_transaksi and id_parameter_transaksi='4013')
    """,
    "initECR": """
        update Deposito set ekuivalen_rate = 0.0 where ekuivalen_rate is null
    """,
    "initBiayaAdmBulanan": """
        update Produk set Biaya_Adm_Bulanan = 0.0 where Biaya_Adm_Bulanan is null
    """,
    "initJumlahHariPerTahun": """
        update Produk set jumlah_hari_pertahun = 3 
        where jumlah_hari_pertahun is null or jumlah_hari_pertahun not in (1,2,3)
    """,
    "initNisbahBonusDasar": """
        update Produk set nisbah_bonus_dasar = 0.0 where nisbah_bonus_dasar is null
    """,
    "copyNisbahDasarProduk": """
        UPDATE RekeningLiabilitas rl SET
          nisbah_dasar = coalesce(p.nisbah_bonus_dasar, 0.0)
        FROM Produk p
        WHERE rl.kode_produk = p.kode_produk
        AND exists ( 
           select  1 from RekeningTransaksi r 
           where r.nomor_rekening = rl.nomor_rekening 
              and r.status_rekening in ( 1, 2)
           )       
    """,
    "initRekeningCustomerBalanceSign": """
        update RekeningTransaksi set balance_sign = 1 
        where Jenis_Rekening_Transaksi in ('C')  and balance_sign is null
    """,
    "initRekeningKasBalanceSign": """
        update RekeningTransaksi set balance_sign = -1 
        where Jenis_Rekening_Transaksi in ('K')  and balance_sign is null
    """,
    "initJumlahAro": """
        update Deposito set jumlah_aro = 0 
        where jumlah_aro is null 
    """,
    "initJumlahBagHas": """
        update RekeningLiabilitas 
        set jumlah_baghas = 0 
        where jumlah_baghas is null 
          and jenis_rekening_liabilitas = 'D'
    """,
    "initTanggalJTDepo_Null_B": """
        update Deposito d 
        set tanggal_jatuh_tempo_berikutnya = add_months(tanggal_buka, (jumlah_aro + 1) * masa_perjanjian) 
        from RekeningLiabilitas rl 
         , RekeningTransaksi rt 
        where rt.nomor_rekening = rl.nomor_rekening
          and rt.nomor_rekening = d.nomor_rekening
          and tanggal_jatuh_tempo_berikutnya is null 
          and rt.status_rekening =1
          and d.periode_perjanjian='B'
    """,
    "initTanggalJTDepo_Null_H": """
        update Deposito d 
        set tanggal_jatuh_tempo_berikutnya = add_days(rl.tanggal_buka, (jumlah_aro+1) * d.masa_perjanjian)
        from RekeningLiabilitas rl 
         , RekeningTransaksi rt 
        where rt.nomor_rekening = rl.nomor_rekening
          and rt.nomor_rekening = d.nomor_rekening
          and tanggal_jatuh_tempo_berikutnya is null 
          and rt.status_rekening =1
          and d.periode_perjanjian='H'
    """,
    "initTanggalBGHDepo_Null_B": """
        update RekeningLiabilitas rl 
        set tanggal_bagi_hasil_berikutnya = add_months(rl.tanggal_buka, 1)
          , tanggal_bagi_hasil_terakhir = tanggal_buka
        from 
         RekeningTransaksi rt ,
         Deposito d 
        where rt.nomor_rekening = rl.nomor_rekening
          and d.nomor_rekening = rl.nomor_rekening 
          and rt.status_rekening=1 
          and rl.tanggal_bagi_hasil_berikutnya is null
          and d.periode_perjanjian='B'
    """,
    "initTanggalBGHDepo_Null_H": """
        update RekeningLiabilitas rl 
        set tanggal_bagi_hasil_berikutnya = add_days(rl.tanggal_buka, (jumlah_baghas +1) * d.masa_perjanjian) 
          , tanggal_bagi_hasil_terakhir = tanggal_buka
        from 
         RekeningTransaksi rt ,
         Deposito d 
        where rt.nomor_rekening = rl.nomor_rekening
          and d.nomor_rekening = rl.nomor_rekening 
          and rt.status_rekening=1 
          and rl.tanggal_bagi_hasil_berikutnya is null
          and d.periode_perjanjian='H'
    """,
    "initTanggalJatuhTempoRencana": """
        update RekeningRencana rr 
        set tanggal_jatuh_tempo = add_months(rl.tanggal_buka, rr.jangka_waktu)
        from RekeningLiabilitas rl 
        where rl.nomor_rekening =rr.nomor_rekening  and rr.tanggal_jatuh_tempo is null 
    """,
    "syncGLAccountName": """
        UPDATE RekeningTransaksi r SET
          nama_rekening  = a.account_name
        FROM Account a
        WHERE a.account_code = r.kode_account
        AND kode_jenis='GL'
        and exists (
         select 1 from Account
         where account_code = r.kode_account 
               and account_name <> r.nama_rekening
         )
    """,
}
