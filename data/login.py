
import polars as pl
from decouple import config
from adlfs import AzureBlobFileSystem

class Auth:
    def __init__(self):
        self.account_name =config("PATH_SECRET_NAME")
        self.account_key =config("PATH_SECRET_KEY")
        self.fs = AzureBlobFileSystem(
            account_name=self.account_name,
            account_key=self.account_key
        )

    def reading(self, azpath: str) -> pl.DataFrame:
        with self.fs.open(azpath, mode='rb') as f:
            df = pl.read_parquet(f)
        return df


# Instância do leitor
reader = Auth()

def ppro() -> pl.DataFrame:return reader.reading(config("AGR")+"/@AGRI_PPRO")
def pprv() -> pl.DataFrame:return reader.reading(config("AGR")+"/@AGRI_PPRV")
def ctvd() -> pl.DataFrame:return reader.reading(config("AGR")+"/@AGRI_CTVD")
def bolc() -> pl.DataFrame:return reader.reading(config("AGR")+"/@AGRI_BOLC")
def bocr() -> pl.DataFrame:return reader.reading(config("AGR")+"/@AGRI_BOCR")
def bolc5() -> pl.DataFrame:return reader.reading(config("AGR")+"/@AGRI_BOLC5")
def unpt() -> pl.DataFrame:return reader.reading(config("AGR")+"/@AGRI_UNPT")
def regr() -> pl.DataFrame:return reader.reading(config("AGR")+"/@PECU_REGR")

