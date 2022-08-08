import pandas as pd
import numpy as np
import cx_Oracle
import pymongo
from datetime import datetime

init = datetime.now()
print(init)

# CONSTANTES
DRYRUN = False # Se DRYRUN = True, o programa não escreve no banco de dados
NROWS = 100000 # Valor para dividir dataframes com N linhas em 
               # (N // NROWS + 1) dataframes com NROWS linhas cada (exceto o último, que fica com N % NROWS linhas)
               # NROWS = None para manter o dataframe com N linhas intacto

print("DRYRUN: %s" %DRYRUN)

pwd = 'des00#cads'
dsn_tns = cx_Oracle.makedsn(host='npaa6635', port='1521', service_name='sdac.petrobras.biz')
conn_analitico = cx_Oracle.connect(user='sdac', password=pwd, dsn=dsn_tns, encoding="UTF-8", nencoding="UTF-8") 

pwd = 'OC0tCWbM'
dsn_tns = cx_Oracle.makedsn(host='npaa3546.petrobras.biz', port='1521', sid='pprompt')
conn_sigitec = cx_Oracle.connect(user='cenpes', password=pwd, dsn=dsn_tns, encoding="UTF-8", nencoding="UTF-8") 


################## FUNÇÕES ####################

# copiei as duas funções abaixo de https://github.com/oracle/python-cx_Oracle/issues/430
# em razão do erro "Python value of type numpy.int64 not supported" ocorrido na carga dos dados de PTxEV
def InConverter(value):
    return int(value)  # or whatever is needed to convert from numpy.int64 to an integer

def InputTypeHandler(cursor, value, num_elements):
    if isinstance(value, np.int64):
        return cursor.var(int, arraysize=num_elements, inconverter=InConverter)
    
# copiei a função chunker de https://stackoverflow.com/questions/17315737/split-a-large-pandas-dataframe
def chunker(df, nrows=NROWS):
    if nrows is not None:
        dfs = []
        for i in range(0, df.shape[0], nrows):  
            dfs.append(df.iloc[i: i + nrows])   
        return dfs
    else:
        return [ df ]  # retorna uma lista para facilitar o loop na função insert_oracle().

def insert_oracle(dfr, sql, nrows=NROWS):

    # se for uma dry run, pula essa função
    if DRYRUN:
        return

    dfs = chunker(df = dfr, nrows = nrows)
    
    for dfr_split in dfs: 
        rows = [tuple(x) for x in dfr_split.values]
        #print(rows)
        cursor = conn_analitico.cursor()
        cursor.inputtypehandler = InputTypeHandler
        try:
            cursor.executemany(sql,rows)
        except cx_Oracle.DatabaseError as e:
            errorObj, = e.args
            print("Row", cursor.rowcount, "has error", errorObj.message)
            conn_analitico.close()
            raise

    conn_analitico.commit()


def unpivot_desembolso(df, chave):

    # pega as DATAS (Planejado e Realizado)
    df = df.rename(columns = {'DT_PREV':'Planejado', 'DT_PGTO':'Realizado'})    
    unpivot = ['Planejado', 'Realizado'] # data
    df_data = pd.melt(df, id_vars=chave, value_vars=unpivot, var_name='CENARIO', value_name='DATA')    
    df = df.drop(columns=unpivot)

    # pega os VALORES (Planejado e Realizado)    
    df = df.rename(columns = {'VL_PREV':'Planejado', 'VL_DESBLS':'Realizado'})    
    unpivot = ['Planejado', 'Realizado'] # valor
    df_valor = pd.melt(df, id_vars=chave, value_vars=unpivot, var_name='CENARIO', value_name='VALOR')
    df = df.drop(columns=unpivot)    
        
    # pega o número da NF (somente Realizado)
    df = df.rename(columns = {'NU_NF':'Realizado'})       
    unpivot = ['Realizado'] # valor
    df_NF = pd.melt(df, id_vars=chave, value_vars=unpivot, var_name='CENARIO', value_name='NOTA_FISCAL')
    df = df.drop(columns=unpivot)    

    chave_merge = chave + ['CENARIO']
    df = pd.merge(df_data, df_valor, on=chave_merge) # merge de DATA e VALOR       
    df = pd.merge(df, df_NF, on=chave_merge, how='left') # merge com NOTA_FISCAL   

    df = df.dropna(subset=['VALOR']) #remove as linhas com VALOR=null
    
    return df

def carrega_desembolso():

    t = datetime.now() 
    tbl = 'SIGITEC_DESEMBOLSO'
    print("Atualizando %s... " %tbl, end='')        
    
    # query fornecida pelo Marcelo Barreiro
    sql = '''
SELECT (CASE WHEN PROC.TP_PROC = '1' THEN 'LEGADO'
             WHEN PROC.NU_PROC IS NOT NULL THEN (SUBSTR(PROC.NU_PROC,0,4) ||'/'|| SUBSTR(PROC.NU_PROC,5,5) ||'-'||SUBSTR(PROC.NU_PROC,10,1))
        END) AS "NU_PROC", -- "Número do Processo"
       ESTADO.NM_EST_PROC ESTADO_PROC, -- "Status do Processo"
       PROC_LEG.NU_CNTR_IBM NU_CNTR_IBM, -- "Contrato IBM"
       (SELECT RTRIM (XMLAGG (XMLELEMENT (E, LPT.NU_PT || ', ') ORDER BY LPT.NU_PT).EXTRACT ('//text()'), ', ')  
        FROM  LISTA_PT LPT 
        WHERE LPT.ID_PROC = PROC.ID_PROC) AS NU_PT, -- "Número PT"
       PROC.NU_JUR NU_JUR, -- "Número Jurídico"
       PROC.NU_SAP NU_SAP, -- "Número SAP"
       CASE WHEN PROP_LEGADO.DS_SIGL_INSTT IS NOT NULL THEN PROP_LEGADO.NM_INSTT ||' / '|| PROP_LEGADO.DS_SIGL_INSTT    ELSE PROP_LEGADO.NM_INSTT END AS PROP, -- "Proponente"
       CASE WHEN CONV_LEGADO.DS_SIGL_INSTT IS NOT NULL THEN CONV_LEGADO.NM_INSTT ||' / '|| CONV_LEGADO.DS_SIGL_INSTT    ELSE CONV_LEGADO.NM_INSTT END AS CONV, -- "Convenente"
       PROC_LEG.VL_ATU_INSTRM_CONTR VL_ATU_INSTRM_CONTR, -- "Valor Total"
       PARCELA_LEGADO.NU_ORD NU_ORD_PREVISTO, -- "Desembolsos Planejados - Número da Parcela"
       PARCELA_LEGADO.DT_PREV  DT_PREV, -- "Desembolsos Planejados - Data de Pagamento"
       PARCELA_LEGADO.VL_PREV VL_PREV, -- "Desembolsos Planejados - Valor Previsto"
       REGISTRO_LEGADO.NU_ORD NU_ORD_REALIZADO, -- "Desembolsos Realizados - Número da Parcela"
       REGISTRO_LEGADO.DT_PGTO DT_PGTO, -- "Desembolsos Realizados - Data de Pagamento"
       REGISTRO_LEGADO.NU_NF NU_NF, -- "Desembolsos Realizados - Número da Nota Fiscal"
       REGISTRO_LEGADO.VL_DESBLS VL_DESBLS -- "Desembolsos Realizados - Valor Pago"
FROM PROCESSO PROC   
INNER JOIN PROCESSO_LEGADO PROC_LEG ON PROC_LEG.ID_PROC_LEGD = PROC.ID_PROC_LEGD   
INNER JOIN ESTADO_PROCESSO ESTADO ON ESTADO.ID_EST_PROC = PROC.ID_EST_PROC    
LEFT JOIN PARCELAS_DESEMBOLSO_LEGADO PARCELA_LEGADO ON PROC_LEG.ID_PROC_LEGD = PARCELA_LEGADO.ID_PROC_LEGD   
LEFT JOIN TRIO_INSTITUICAO_LEGADO TRIO_INSTT_LEGADO ON TRIO_INSTT_LEGADO.ID_PROC_LEGD = PROC_LEG.ID_PROC_LEGD AND TRIO_INSTT_LEGADO.FL_PRINC = 1   
LEFT JOIN INSTITUICAO PROP_LEGADO ON PROP_LEGADO.ID_INSTT = TRIO_INSTT_LEGADO.ID_INSTT_PROPN   
LEFT JOIN INSTITUICAO CONV_LEGADO ON CONV_LEGADO.ID_INSTT = TRIO_INSTT_LEGADO.ID_INSTT_CONVN   
LEFT JOIN REGISTRO_PAGAMENTO_LEGADO REGISTRO_LEGADO ON ((REGISTRO_LEGADO.ID_PROC_LEGD = PROC_LEG.ID_PROC_LEGD AND PARCELA_LEGADO.ID_PARCE_DESBLS_LEGD IS NULL) OR   
           (PARCELA_LEGADO.NU_ORD = REGISTRO_LEGADO.NU_ORD AND PARCELA_LEGADO.ID_PROC_LEGD = REGISTRO_LEGADO.ID_PROC_LEGD AND PARCELA_LEGADO.ID_PROC_LEGD IS NOT NULL))
WHERE ESTADO.TX_SIGL_EST_PROC IN ('ANA', 'ECO', 'EME', 'EXE', 'LEX', 'EHA', 'ENC', 'LEN', 'LEE', 'LET')
  AND PROC.ID_SIST = (SELECT ID_SIST FROM SISTEMA SIST WHERE SIST.CD_SIST = 'SIGITEC') 
UNION 
SELECT (CASE WHEN PROC.TP_PROC = '1' THEN 'LEGADO'
             WHEN PROC.NU_PROC IS NOT NULL THEN (SUBSTR(PROC.NU_PROC,0,4) ||'/'|| SUBSTR(PROC.NU_PROC,5,5) ||'-'||SUBSTR(PROC.NU_PROC,10,1))
        END) AS "NU_PROC", -- "Número do Processo"
       ESTADO.NM_EST_PROC ESTADO_PROC, -- "Status do Processo"
       '', -- "Contrato IBM"
       (SELECT RTRIM (XMLAGG (XMLELEMENT (E, LPT.NU_PT || ', ') ORDER BY LPT.NU_PT).EXTRACT ('//text()'), ', ') 
        FROM  LISTA_PT LPT 
        WHERE LPT.ID_PROC = PROC.ID_PROC) AS NU_PT, -- "Número PT"
       PROC.NU_JUR, -- "Número Jurídico"
       PROC.NU_SAP, -- "Número SAP"
       CASE WHEN PROP.DS_SIGL_INSTT IS NOT NULL THEN PROP.NM_INSTT ||' / '|| PROP.DS_SIGL_INSTT    ELSE PROP.NM_INSTT END AS PROP, -- "Proponente"
       CASE WHEN CONV.DS_SIGL_INSTT IS NOT NULL THEN CONV.NM_INSTT ||' / '|| CONV.DS_SIGL_INSTT    ELSE CONV.NM_INSTT END AS CONV, -- "Convenente"  
       (SELECT SUM(REC.VL_TOT) FROM RECURSO REC WHERE REC.ID_ORC = PROC.ID_ORC) AS VL_TOT, -- "Valor Total"
       PARCELA.NU_ORD, -- "Desembolsos Planejados - Número da Parcela"
       PARCELA.DT_PREV, -- "Desembolsos Planejados - Data de Pagamento"
       PARCELA.VL_PREV, -- "Desembolsos Planejados - Valor Previsto"
       PARCELA.NU_ORD, -- "Desembolsos Realizados - Número da Parcela"
       REGISTRO.DT_FATR, -- "Desembolsos Realizados - Data de Pagamento"
       REGISTRO.NU_NOTA_FISC, -- "Desembolsos Realizados - Número da Nota Fiscal"
       REGISTRO.VL_FATR -- "Desembolsos Realizados - Valor Pago" 
FROM PROCESSO PROC  
INNER JOIN MARCO_DESEMBOLSO MARCO ON MARCO.ID_ORC = PROC.ID_ORC  
LEFT JOIN PARCELAS_DESEMBOLSO PARCELA ON PARCELA.ID_MARCO_DESBLS = MARCO.ID_MARCO_DESBLS  
LEFT JOIN TRIO_INSTITUICAO TRIO_INSTT ON TRIO_INSTT.ID_TRIO_INSTT = PARCELA.ID_TRIO_INSTT AND TRIO_INSTT.FL_PRINC = 1  
LEFT JOIN INSTITUICAO PROP ON PROP.ID_INSTT = TRIO_INSTT.ID_INSTT_PROPN  
LEFT JOIN INSTITUICAO CONV ON CONV.ID_INSTT = TRIO_INSTT.ID_INSTT_CONVN  
INNER JOIN ESTADO_PROCESSO ESTADO ON ESTADO.ID_EST_PROC = PROC.ID_EST_PROC  
LEFT JOIN REGISTRO_PAGAMENTO REGISTRO ON REGISTRO.ID_PARCE_DESBLS = PARCELA.ID_PARCE_DESBLS  
LEFT JOIN PROCESSO_LEGADO PROC_LEG ON PROC.ID_PROC_LEGD = PROC_LEG.ID_PROC_LEGD   
WHERE ESTADO.TX_SIGL_EST_PROC IN ('ANA', 'ECO', 'EME', 'EXE', 'LEX', 'EHA', 'ENC', 'LEN') 
  AND PROC.ID_SIST = (SELECT ID_SIST FROM SISTEMA SIST WHERE SIST.CD_SIST = 'SIGITEC')
    '''

    df = pd.read_sql_query(sql, conn_sigitec)    
    
    # colunas...
    # ['NU_PROC','ESTADO_PROC','NU_CNTR_IBM','NU_PT','NU_JUR','NU_SAP','PROP','CONV','VL_ATU_INSTRM_CONTR', 
    #  'NU_ORD_PREVISTO', 'DT_PREV', 'VL_PREV', 'NU_ORD_REALIZADO', 'DT_PGTO', 'NU_NF', 'VL_DESBLS']
    
    # tratando numero da parcela...
    boolean = (df['NU_ORD_PREVISTO'].isna()) & (df['NU_ORD_REALIZADO'].notna())
    df.loc[boolean, 'NU_ORD_PREVISTO'] = df['NU_ORD_REALIZADO']    
    
    boolean = df['NU_ORD_PREVISTO'].isna()
    df.loc[boolean, 'NU_ORD_PREVISTO'] = 0

    df = df.drop(columns=['NU_ORD_REALIZADO'])         
    df = df.rename(columns = {'NU_ORD_PREVISTO':'NUM_PARCELA'})    

    # tratando tipo de dado de NU_SAP
    boolean = df['NU_SAP'].notna()
    df.loc[boolean, 'NU_SAP'] = df['NU_SAP'].astype(str).str[:-2] #get all but the two last character 
   
    manter = ['NU_PROC', 'NUM_PARCELA', 'NU_CNTR_IBM', 'NU_SAP', 'NU_JUR']
    df = unpivot_desembolso(df, manter)
    
    df['VALOR'] = df['VALOR'] / 1000 # no Analítico, os valores monetários sempre são em R$ mil          
    
    df = df.where(df.notnull(), None)
    
    df = df[['NU_PROC','NU_SAP','NU_JUR','NU_CNTR_IBM','NOTA_FISCAL','CENARIO','NUM_PARCELA','DATA','VALOR']]
    df = df.sort_values(by=['CENARIO','NU_PROC','NU_SAP','NU_CNTR_IBM','NUM_PARCELA'])
    df = df.reset_index(drop=True)    
       
    if DRYRUN: 
        return
    
    conn_analitico.cursor().execute('DELETE FROM %s' %tbl)
    conn_analitico.commit()
    #print("Dados de %s deletados!" %tbl)
    
    sql = '''INSERT INTO %s (
                NUM_PROCESSO, NUM_SAP, NUM_JURIDICO, CONTRATO_IBM, 
                NOTA_FISCAL, CENARIO, NUM_PARCELA, DATA, VALOR
                ) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9)''' %tbl
    
    insert_oracle(df, sql)

    print(str((datetime.now()-t).total_seconds()) + " segundos" )                
    
def carrega_processo():
    
    t = datetime.now() 
    tbl = 'SIGITEC_PROCESSO'
    print("Atualizando %s... " %tbl, end='')    
    
    df = pd.read_sql_query("SELECT * FROM VW_RELATORIO_STATUS", conn_sigitec)
    #print(df.columns)

    df = df.rename(columns = {'CD_ENTREGA_VALOR':'COD_EV'})  
    df['COD_EV'] = df['COD_EV'].str.strip()
    
    # NVL(NM_EST_ADIT_DENC, NM_EST_ADIT_RESC), -- "Estado da Denúncia / Rescisão"
    # NVL(DT_EST_ADIT_DENC, DT_EST_ADIT_RESC), -- "Data da Denúncia / Rescisão - Desde"
    df.loc[df['NM_EST_ADIT_DENC'].isna(), 'NM_EST_ADIT_DENC'] = df['NM_EST_ADIT_RESC']
    df.loc[df['DT_EST_ADIT_DENC'].isna(), 'DT_EST_ADIT_DENC'] = df['DT_EST_ADIT_RESC']
    
    df = df[[
        'TP_PRIORIDADE','Consorcio','DT_INI_SIC','NM_TP_FLUXO','NM_EST_PROC','NM_INV_DIVULG', #6
        'TP_INSTRU_ENQD', 'TX_TITL', 'DIP_SIC', 'DT_INI_DIP_SIC', 'TIPO_PROC', #11
        'NM_TP_MOEDA', 'NM_INSTRM_CONTR', 'NM_CONTR_RESP', 'NM_ANALISTA', 'NU_JUR', 'NU_SAP', #17
        'NU_PT', 'NU_ELEMT_PEP', 'COD_EV', #20
        'NU_OBJT_CUST','NM_INSTT_PROPN','NM_INSTT_CONVN','NM_COORD','NM_GERN_TECNI','NM_GERN_GERAL',#26
        'NM_AREA_PROGR','NM_NUCL_REDE','NM_INTERLOC_TECNI','NM_GERN_CONTR','NM_TRAMT_ANP',#31
        'NM_EST_OBJT','DT_EST_OBJT','NM_EST_REFORM_FINN','DT_EST_REFORM_FINN',#35
        'NM_EST_ADIT_VALR','DT_EST_ADIT_VALR','NM_EST_ADIT_ESCP','DT_EST_ADIT_ESCP',#39
        'NM_EST_ADIT_PRAZO','DT_EST_ADIT_PRAZO','NM_EST_ADIT_DENC','DT_EST_ADIT_DENC',#43
        'EST_DEC_PREV_CONF','EST_DEC_PER_CONF','EST_DES_COM_SUP','NM_EST_PC',#47
        'NM_INDC_ANALISE_PC','DT_EST_PC','NM_EST_AUTRZ_PGTO','DT_INI','DT_FIM',#52
        'VL_CONTR','VL_DESBLS','NU_PARCE_PREV','NU_PARCE_PGTO',#56
        'DATA_PAG_ULT_FATR','VL_PAG_ULT_FATR','VL_PROX_PARC','NU_PROC','CD_SOLIC_INI_CONTR' #61
        ]]
    
    for col in ['VL_CONTR', 'VL_DESBLS', 'VL_PAG_ULT_FATR', 'VL_PROX_PARC']: 
        df[col] = df[col] / 1000 # no Analítico, os valores monetários sempre são em R$ mil          
    
    df = df.where(df.notnull(), None)
    
    # ordena e reseta índice
    df = df.sort_values(by=['NU_PROC'])
    df = df.reset_index(drop=True)
    
    if DRYRUN: 
        return

    conn_analitico.cursor().execute('DELETE FROM %s' %tbl)
    conn_analitico.commit()
    #print("Dados de %s deletados!" %tbl)   

    sql = '''INSERT INTO %s (
            PRIORIDADE, CONSORCIO, DT_INICIO_SIC_AEP, TIPO_FLUXO, STATUS, TIPO_INVEST_DIVULG,
            INTENCAO_ENQDRM, OBJETO_CONTRAT_TITULO, DIP_SIC_AEP, DT_EMISSAO_DIP_SITC, TIPO_CONTRATACAO,
            TIPO_MOEDA, TIPO_INSTR_CONTRATUAL, CONTRATADOR_APOIADOR, ANALISTA_CONTRAT, NUM_JURIDICO, NUM_SAP, 
            NUM_PT, ELEMENTO_PEP, CODIGO_ENTREGA,
            OBJETO_CUSTO, PROPONENTE, CONVENENTE, COORDENADOR, GERENCIA_TECNICA, GERENCIA_GERAL,
            AREA_PROGRAMA_TECN, REDE_TEMATICA_NUCLEO, INTERLOCUTOR_TECNICO, GERENTE_CONTRATO, TRAMITE_ANP_PROPOSTA,
            STATUS_SIC_AEP_PROPOSTA_CONTRATO, STATUS_SIC_AEP_PROPOSTA_CONTRATO_DESDE, 
            STATUS_REFORM_FINANC, STATUS_REFORM_FINANC_DESDE, 
            STATUS_ADITIVO_VALOR, STATUS_ADITIVO_VALOR_DESDE, STATUS_ADITIVO_ESCOPO, STATUS_ADITIVO_ESCOPO_DESDE,
            STATUS_ADITIVO_PRAZO, STATUS_ADITIVO_PRAZO_DESDE, STATUS_DENUNCIA_RESCISAO, STATUS_DENUNCIA_RESCISAO_DESDE,
            STATUS_DECL_PREVIA_CONF, STATUS_DECL_PERIODICA_CONF, STATUS_DESIGN_CMS_SUPERVISAO, STATUS_PC,
            INDICACAO_ANALISE_PC, STATUS_PC_DESDE, STATUS_PAGAMENTO, DT_INICIO, DT_TERMINO, 
            VALOR_PROJETO, VALOR_DESEMB, NUM_PARCELAS_PREVISTAS, NUM_PARCELAS_PAGAS,
            DT_ULTIMA_PARCELA_DESEMB, VALOR_ULTIMO_PAGAMENTO, VALOR_PROX_PARCELA, NU_PROC, CD_SOLIC_INI_CONTR            
            
            ) VALUES ( :1, :2, :3, :4, :5, :6, :7, :8, :9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,
                      :21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,
                      :41,:42,:43,:44,:45,:46,:47,:48,:49,:50,:51,:52,:53,:54,:55,:56,:57,:58,:59,:60,:61)''' %tbl

    #print(df.head())
    insert_oracle(df, sql)

    print(str((datetime.now()-t).total_seconds()) + " segundos" )             
    
def carrega_status_pc():

    t = datetime.now() 
    tbl = 'SIGITEC_STATUS_PC'
    print("Atualizando %s... " %tbl, end='')
    
    df = pd.read_sql_query("SELECT * FROM VW_RELATORIO_STATUS_PC", conn_sigitec)
    #print(df.columns)

    df = df [[
        'DT_INI_ANALISE','DT_CONC_ANALISE','NU_QTD_PEND','ID_PROC','ID_GR_GER','ID_GR_TEC','ID_PROP','ID_CONV',#8
        'NU_SAP','NU_PROC','TP_PROC','NU_JUR','NM_INSTT_PROP','NM_INSTT_CONV','NM_GERN_TECNI','NM_GERN_GERAL',#16
        'NM_INTERLOC_TECN','NM_GERN_TECN','DT_INI','DT_FIM','NU_PARC','DT_COMPMS','TP_ANALISE',#23
        'NM_PESS_ANALISTA','NM_EST_PC','DT_EST_PC','TP_PEND','NM_EST_TECN','DT_EST_TECN','NM_RESP_ATUAL',#30
        'VL_CONTR','VL_DESBLS','VL_TOTAL_PC','NU_PARCE_PREV','NU_PARCE_PGTO','VL_PROX_PARC','DT_SUBM' #37
    ]]
    
    for col in ['VL_CONTR', 'VL_DESBLS', 'VL_TOTAL_PC', 'VL_PROX_PARC']: 
        df[col] = df[col] / 1000 # no Analítico, os valores monetários sempre são em R$ mil          
    
    df = df.where(df.notnull(), None)

    if DRYRUN: 
        return

    conn_analitico.cursor().execute('DELETE FROM %s' %tbl)
    conn_analitico.commit()
    #print("Dados de %s deletados!" %tbl)    
    
    sql = '''INSERT INTO %s (
            DT_INI_ANALISE, DT_CONC_ANALISE, NU_QTD_PEND, ID_PROC, ID_GR_GER, ID_GR_TEC, ID_PROP, ID_CONV,
            NU_SAP, NU_PROC, TP_PROC, NU_JUR, NM_INSTT_PROP, NM_INSTT_CONV, NM_GERN_TECNI, NM_GERN_GERAL,
            NM_INTERLOC_TECN, NM_GERN_TECN, DT_INI, DT_FIM, NU_PARC, DT_COMPMS, TP_ANALISE, 
            NM_PESS_ANALISTA, NM_EST_PC, DT_EST_PC, TP_PEND, NM_EST_TECN, DT_EST_TECN, NM_RESP_ATUAL, 
            VL_CONTR, VL_DESBLS, VL_TOTAL_PC, NU_PARCE_PREV, NU_PARCE_PGTO, VL_PROX_PARC, DT_SUBM 
            
            ) VALUES ( :1, :2, :3, :4, :5, :6, :7, :8, :9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,
                      :21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37)''' %tbl
    

    #print(df.head())    
    insert_oracle(df, sql)
    
    print(str((datetime.now()-t).total_seconds()) + " segundos" )    


def carrega_ambiente_competitividade():
    t = datetime.now() 
    tbl = 'SIGITEC_AMBIENTE_COMPET'
    print("Atualizando %s... " % tbl, end='')    
    
    #a consulta a coleção do MongoDB nos retorna um objeto pymongo parecido como um dicionário. vamos percorrê-lo para montar
    #o dataframe.
    #a consulta busca dados em 2 coleções: opportunities e draftproposals
    
    #dados da conexao
    str_conexao = 'mongodb+srv://usb4u:7g1UykL51a@rscorpprd01.petrobras.biz/sb4u?ssl=false'
    porta = 27017
    client = pymongo.MongoClient(str_conexao, porta)
    db = client.sb4u #bd

    ################## funções ##################
    def get_oportunidade(nro_oportunidade):
        '''Retorna o código com o prefixo OP'''
        try:
            #return 'OP%s' % str(nro_oportunidade).rjust(6, '0')
            return 'OP%s' % str(nro_oportunidade)
        except:
            return np.nan

    def get_vencedor(lista_dicionario):
        '''Retorna todas as parcerias vencedoras concatenadas por ";"'''
        try:
            vencedor = ''
            for i in lista_dicionario:
                vencedor = vencedor + '; ' + i['nome']
            return vencedor[2:]
        except:
            return np.nan
    
    def get_qtd_parceiras(tipo_chamada, qtd):
        '''Se 'Seleçao Tematica', retorna a quantidade de parceiras ";"'''
        try:
            if tipo_chamada == 'Seleçao Tematica':
                return qtd
            return np.nan
        except:
            return np.nan
        
    ################## funções fim ###############
    
    #pipeline ajustado conforme consulta fornecida
    #ajuste: troca de '$$partnerships.winner' por '$$partnerships.selected'
    pipeline = [
        {
          '$lookup': {
            'from': 'draftproposals',
            'let' : {'opportunity_id' : '$_id'},
            'pipeline': [
                 { '$match':
                     { '$expr':
                        { '$and':
                           [
                             { '$eq': [ '$opportunityId',  '$$opportunity_id' ] },
                             { '$eq': [ '$status', 'Submitted' ] }
                           ]
                        }
                     }
                  },
                  { '$project': { '_id': 0,  } }
            ], 'as': 'numberDraftProposals'
          },
        },
        { '$project': {  '_id': 1,
                        'oportunidade' : '$opportunityNumber', 
                        'titulo_oportunidade' : '$name',
                        'solucao_tecnologica' : '$technologicalSolution.title',
                        'entrega' : '$technologicalSolution.delivery.title',
                        'crl' : '$intendedCRL',
                        'trl' : '$intendedTRL',
                        'aporte_maximo' : '$financialSupport',
                        'tipo_chamada': {
                                                '$switch': {
                                                    'branches' : [
                                                    {
                                                        'case': {'$eq' : ['$opportunityMethod', 'PublicSelection']}, 
                                                        'then': 'Seleçao Publica'
                                                    },
                                                    {
                                                        'case': {'$eq' : ['$opportunityMethod', 'ThematicSelection']}, 
                                                        'then': 'Seleçao Tematica'
                                                    },
                                                     {
                                                        'case': {'$eq' : ['$opportunityMethod', 'DirectSelection']}, 
                                                        'then': 'Seleçao Direta'
                                                    }
                                                    ],
                                                    'default': ''
                                                }
                         },
                      'qtd_parceria' : {'$size': {'$ifNull': ['$partnerships', [] ] }},
                      'situacao': {
                                                '$switch': {
                                                    'branches' : [
                                                    {
                                                        'case': {'$eq' : ['$status', 'InPreparation']}, 
                                                        'then': 'Em Elaboraçao'
                                                    },
                                                    {
                                                        'case': {'$eq' : ['$status', 'Submitted']}, 
                                                        'then': 'Submetida'
                                                    },
                                                    {
                                                        'case': {'$eq' : ['$status', 'Approved']}, 
                                                        'then': 'Aprovada'
                                                    },
                                                    {
                                                        'case': {'$eq' : ['$status', 'ApprovedCompetitiveness']}, 
                                                        'then': 'Competitividade Aprovada'
                                                    },
                                                    {
                                                        'case': {'$eq' : ['$status', 'Canceled']}, 
                                                        'then': 'Cancelada'
                                                    },
                                                    {
                                                        'case': {'$eq' : ['$status', 'Accept']}, 
                                                        'then': 'Aceitada'
                                                    },
                                                    {
                                                        'case': {'$eq' : ['$status', 'Finished']}, 
                                                        'then': 'Finalizada'
                                                    },
                                                    ],
                                                    'default': ''
                                                }
                         },
                         'data_criacao' : '$createdAt',
                         'interlocutor' : '$interlocutor.name',
                         'gerente_tecnico' : '$interlocutorManager.name',
                         'interlocutor_par' : '$technicalInterlocutorPair.name',
                         'gerente_par' : '$managerPair.name',
                         'prazo_final_submissao' : '$deadlines.sendingDraftProposalsDate',
                         'qtd_pre_prop_submetidas' : {'$size': {'$ifNull': ['$numberDraftProposals', [] ] }},
                         'qtd_vencedor': {
                                                                 '$size': { '$ifNull':[
                                                                     {'$filter':{
                                                                     'input': '$partnerships',
                                                                     'as': 'partnerships',
                                                                     'cond': {'$eq' : ['$$partnerships.selected', True]}
                                                                 }},[]]}
                                                                },
                         'parceria_vencedora': {
                             '$map':{
                                 'input':{
                                     '$filter':{
                                         'input': '$partnerships',
                                         'as': 'partnerships',
                                         'cond': {'$eq' : ['$$partnerships.selected', True]}
                                         }
                                     },
                                     'as': 'partnerships',
                                     'in': {'nome': '$$partnerships.name'}
                                 }
                             }
                      } 
        } 
    ]
    
    #execulta consulta agregada
    base = db.opportunities.aggregate(pipeline)
    
    #vamos percorrer a consulta para montar o dataframe
    chaves = ['oportunidade', 'titulo_oportunidade', 'solucao_tecnologica', 'entrega', 'crl', 'trl', 'aporte_maximo',
              'tipo_chamada', 'qtd_parceria', 'situacao', 'data_criacao', 'interlocutor', 'gerente_tecnico', 
              'interlocutor_par', 'gerente_par', 'prazo_final_submissao', 'qtd_pre_prop_submetidas', 'qtd_vencedor',
              'parceria_vencedora']

    df = None
    for i in base:
        valores = []
        for j in chaves:
            try:
                valor = i[j]
            except:
                valor = np.nan
            valores.append(valor)
        dados = [valores]
        temp = pd.DataFrame(dados, columns = chaves)
        df = pd.concat([df, temp], ignore_index = True)
    
    #ajuste dos dados para carga
    df['id_op'] = df['oportunidade']
    df['cod_op'] = df.apply(lambda x: get_oportunidade(x['oportunidade']), axis = 1) # adiciona o prefixo OP
    df['aporte_maximo'] = df['aporte_maximo'] / 1000 # no Analítico, os valores monetários sempre são em R$ mil
    df['qtd_parceria'] = df.apply(lambda x: get_qtd_parceiras(x['tipo_chamada'], x['qtd_parceria']), axis = 1) # somente se tipo chamada for 'Seleçao Tematica'
    df['parceria_vencedora'] = df.apply(lambda x: get_vencedor(x['parceria_vencedora']), axis = 1) # concatena as parcerias vencedoras por ';'

    # setando ST como nulo, pois o correto é pegar a ST a partir da Entrega
    df['solucao_tecnologica'] = None
    
    # separando o COD_EV do título da entrega..
    boolean = df['entrega'].isna()
    df.loc[boolean, 'entrega'] = 'tit EV-dummy'   
    
    df['cod_ev']  = df['entrega'].str[-8:] #get the last eight char
    df['entrega'] = df['entrega'].str[:-8].str.strip() #get all but the last eight char
    
    boolean = (df['cod_ev']=='EV-dummy')
    df.loc[boolean, 'cod_ev'] = None
    df.loc[boolean, 'entrega'] = None  
    ###
    
    df = df.sort_values(by=['id_op'])
    #df = df.reset_index(drop=True)    
    
    #substitui valores NaN por None
    df = df.where(df.notnull(), None)
    
    df = df[['id_op', 'cod_op', 'titulo_oportunidade', 'solucao_tecnologica', 'cod_ev','entrega', 'crl', 'trl', 
             'aporte_maximo', 'tipo_chamada', 'qtd_parceria', 'situacao', 'data_criacao', 
             'interlocutor', 'gerente_tecnico', 'interlocutor_par', 'gerente_par', 
             'prazo_final_submissao', 'qtd_pre_prop_submetidas', 
             'qtd_vencedor', 'parceria_vencedora']]
    
    #inserção do df no bd analitico
    if DRYRUN: 
        return
    
    #delete antes de inserir os novos dados
    conn_analitico.cursor().execute('DELETE FROM %s' % tbl)
    conn_analitico.commit()
    
    sql = '''INSERT INTO %s (ID, COD_OP, TITULO_OPORTUNIDADE, SOLUCAO_TECNOLOGICA, COD_EV, ENTREGA, CRL, TRL, 
                             APORTE_MAXIMO_PB, TIPO_CHAMADA, QTDE_PARCEIRAS, SITUACAO, DATA_CRIACAO, 
                             INTERLOCUTOR, GERENTE_TECNICO, INTERLOCUTOR_PAR, GERENTE_PAR, 
                             PRAZO_FINAL_SUBM_PRE_PROPOSTA, QTDE_PRE_PROPOSTAS_SUBM,
                             QTDE_PARCEIRAS_VENCEDORAS, PARCEIRAS_VENCEDORAS
            ) VALUES ( :1, :2, :3, :4, :5, :6, :7, :8, :9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21)''' % tbl
    
    #chamada da função de inserção no bd
    insert_oracle(df, sql)
    
    #para visualização do df no notebook
    #pd.set_option('display.max_rows', 500)
    #display(df)
    
    print(str((datetime.now()-t).total_seconds()) + " segundos" )   
    
############

carrega_desembolso()
carrega_processo()
carrega_status_pc()
carrega_ambiente_competitividade()

print("*** Tabelas do SIGITEC atualizadas com sucesso!")

conn_sigitec.close()
conn_analitico.close()
