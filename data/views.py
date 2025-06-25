from django.http import JsonResponse
from django.shortcuts import render
from .login import ppro, pprv, unpt, bolc, bolc5,bocr, regr
import polars as pl


# precisa ser formatado uma forma de puxar um codigo e vir periodo e 
# contratos de entrada de uma forma que consigamos digerir na PipeLine

xppro = pprv()   # Cab_Periodo de Produção
xpprv = ppro()   # Linhas_Periodo de Produção
xregr = regr()   # Romaneio_Entrada 
xunpt  = unpt()  # Cadastros de Talhão, pra joins()
xbolc5 = bolc5() # Cadastros de Talhão, pra joins()
Xbolc = bolc()


def boletim_colheita () -> pl.DataFrame:
    x5 = xbolc5    
    x = Xbolc
    
    x = x.filter(pl.col('U_CodPeriodoProducao')=='P002')## <!!!!> Aqui a gente vai filtrar o Periodo, posteriormente vai virar Cod.Externo
    
    df = x.join(x5, on='DocEntry', how='inner')
    
    df = df.group_by('U_CodTalhao').agg([
        pl.col('U_AreaColhida').cast(pl.Float64).sum().round(2)
    ])
    
    return df

def periodo_prod() -> pl.DataFrame:
    Qppro = xppro
    Qpprv = xpprv
    Qppro = Qppro.filter(pl.col('Code')=='P002') ## <!!!!> Aqui a gente vai filtrar o Periodo, posteriormente vai virar Cod.Externo

    df = Qppro.join(Qpprv, on='Code', how='inner')
    
    df = df.group_by(['U_CodTalhao', 'U_DscTalhao']).agg([pl.col('U_AreaPlanta').cast(pl.Float64).round(2).sum()])

    return df

def romaneio_entrada () -> pl.DataFrame:
    CadTalhao = xunpt 
    CadTalhao = CadTalhao.select(['Name', 'Code', 'U_DscUnPrSet', 'U_QtdAreaPro'])
    
    df = xregr
    df = df.filter(pl.col('U_CodRegistroCompra')=='29')
    df = (
        df.select([
            'DocEntry','U_CodTalhao','U_CodSafra', 'U_DscSafra', 'U_DataEntrada', 
            'U_CodRegistroCompra', 'U_NumeroBoletim', 'U_PesoNota', 
            'U_PesoBruto', 'U_PesoTara', 'U_PesoLiquido',
            'U_PesoLiquidoDesc', 'U_Diferenca'
        ])
        .with_columns(
            
            (pl.col('U_Diferenca').cast(pl.Float64).round(2)),
            (pl.col('U_PesoBruto').cast(pl.Float64).round(2)),
            (pl.col('U_PesoLiquido').cast(pl.Float64).round(2)),
            (pl.col('U_PesoLiquidoDesc').cast(pl.Float64).round(2)),
            (pl.col('U_PesoLiquido').cast(pl.Float64) / 60).cast(pl.Float64).alias('PesoBruto/Sc'),
            (pl.col('U_PesoLiquidoDesc').cast(pl.Float64)/60).cast(pl.Float64).alias('PesoLiquido/Sc')
        )
    )   
    df = df.join(CadTalhao, left_on='U_CodTalhao', right_on='Code', how='inner')
    
    return df

def agrupamento ()-> pl.DataFrame:
    boleIN = boletim_colheita ()
    romaIN = romaneio_entrada ()
    perPRO = periodo_prod ()
    
    df_romaIN = romaIN.group_by(['Name','U_CodTalhao']).agg(
        [
            pl.col('U_PesoLiquido').sum().round(2),
            pl.col('U_PesoLiquidoDesc').sum().round(2),
            pl.col('PesoBruto/Sc').sum().round(2),
            pl.col('PesoLiquido/Sc').sum().round(2),
            pl.col('U_Diferenca').sum().round(2).alias('Descontos'),
            pl.col('U_PesoBruto').sum().round(2),
        ])    
    agp = perPRO.join(df_romaIN, left_on='U_CodTalhao', right_on='U_CodTalhao', how='inner')    

    df_cleaner = agp.join(boleIN, on='U_CodTalhao', how='inner')
    
    
    df_final = (df_cleaner.select(
                                'U_CodTalhao','U_DscTalhao', 'U_AreaPlanta', 'U_AreaColhida', 
                                'U_PesoLiquido', 'U_PesoLiquidoDesc', 'PesoBruto/Sc', 'PesoLiquido/Sc', 'Descontos'   
                                )
                .with_columns((pl.col('PesoLiquido/Sc').cast(pl.Float64) / pl.col('U_AreaColhida').cast(pl.Float64)).cast(pl.Float64).round(2).alias('Sc/Ha - Liquido'),
                              (pl.col('PesoBruto/Sc') / pl.col('U_AreaColhida')).round(2).alias('Sc/Ha - Bruto')
                              )
                
                )
    
    return df_final


def home (request):
          x = agrupamento()
          x = x.to_dicts()
          
          return render(request, "index.html", {
                    "dashboard": x       
          } )

