import pandas as pd
from xlsxwriter import *
import pandasql as ps

def fusion(repmast, key1 =None, beghist=None,endhist=None, repslave=None):

        #Intégration et préparation du fichier Master
    master = pd.read_csv(repmast,error_bad_lines=False,sep="|",low_memory=False)
    master[beghist] = pd.to_datetime(master[beghist])
    master[endhist] = master[endhist].replace('31/12/2999','31/12/2100')
    master[endhist] = pd.to_datetime((master[endhist]))
    col = []
    for val in master.columns:

        val = str(val).replace(" ","_")
        col.append(val)

    master.columns = col

        #Intégration et préparation du fichier Slave
    slave = pd.read_csv(repslave,error_bad_lines=False,sep="|",low_memory=False)
    slave[endhist] = slave[endhist].replace('31/12/2999', '31/12/2100')
    slave[endhist] = slave[endhist].apply(lambda x: str(x[6:10]) + '/' + str(x[3:5]) + '/' + str(x[0:2]))  # Convertion au format date américain
    slave[beghist] = slave[beghist].apply(lambda x: str(x[6:10]) + '/' + str(x[3:5]) + '/' + str(x[0:2]))  # Convertion au format date américain
    slave[beghist] = pd.to_datetime(slave[beghist])
    slave[endhist] = pd.to_datetime((slave[endhist]))

    col = []
    for val in slave.columns:
        val = str(val).replace(" ", "_")
        col.append(val)

    slave.columns = col
    print(slave.columns)
    sql = "Select * from master m left join slave s on (m.Personnel_number = s.Personnel_number) where s.Start_Date>=m.Start_Date and m.End_Date>=s.End_Date;"
    newdf = ps.sqldf(sql, locals())
    print(newdf)
    writer = pd.ExcelWriter(r'C:\Users\Sabri\Desktop\Result.xlsx', engine='xlsxwriter')
    newdf.to_excel(writer, sheet_name="result")
    writer.save()
    return print("cool")

toto = fusion(r"C:\Users\Sabri\Desktop\CONTRAT_02.TXT",beghist="Start Date",endhist="End Date",repslave=r"C:\Users\Sabri\Desktop\CONTRAT_01.TXT")