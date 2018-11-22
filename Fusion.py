import pandas as pd
import pandasql as ps


class fusion:
    def __init__(self, imported_files, start_hist, end_hist, id_employee):
        self.files_repertories = imported_files  # Imported files repertories
        self.data = {}
        self.master = pd.DataFrame # Dataframe that contain the main file that will define the beginning and the end
        self.start_hist = start_hist #Text label of start dates in files
        self.end_hist = end_hist #Text label of end dates in files
        self.id_emp = id_employee #Text label of employees id
        self.accepted_interval = pd.DataFrame
        self.__loading()

    def __loading(self):
        print(self.files_repertories.values)

        for key, value in self.files_repertories.items():
            try:
                self.data[key] = pd.read_csv(value, error_bad_lines=False, sep="|", low_memory=False)
            except:
                raise Exception('an error occur during the loading of the csv files')

    def data_preparation(self):
        i = 0
        for dataframe in self.data.values():

            dataframe[self.end_hist] = dataframe[self.end_hist].replace('31/12/2999',
                                                                        '31/12/2100')  # replace out of range dates for a datetime format by acceptable ones
            dataframe[self.end_hist] = dataframe[self.end_hist].apply(
                lambda x: str(x[6:10]) + '/' + str(x[3:5]) + '/' + str(x[0:2]))  # Change euro format date to us format
            dataframe[self.start_hist] = dataframe[self.start_hist].apply(
                lambda x: str(x[6:10]) + '/' + str(x[3:5]) + '/' + str(x[0:2]))  # Change euro format date to us format
            dataframe[self.end_hist] = pd.to_datetime(dataframe[self.end_hist])  # Conversion to datetime format
            dataframe[self.start_hist] = pd.to_datetime((dataframe[self.start_hist]))  # Conversion to datetime format

            col = []  # Replace space to "_" to prepare the futur SQL operation
            for val in dataframe.columns:
                val = str(val).replace(" ", "_")
                col.append(val)

            dataframe.columns = col

        self.start_hist = str(self.start_hist).replace(" ", "_")
        self.end_hist = str(self.end_hist).replace(" ", "_")
        self.id_emp = str(self.id_emp).replace(" ", "_")
        self.master = list(self.data.values())[0] # Assign value for the master file

    def date_preparation(self):
        min_date = self.master[[self.id_emp, self.start_hist]].sort_values(self.start_hist).groupby([self.id_emp], as_index=False).min()
        max_date = self.master[[self.id_emp, self.end_hist]].sort_values(self.end_hist).groupby([self.id_emp], as_index=False).max()
        self.accepted_interval = pd.merge(min_date, max_date[[self.id_emp,self.end_hist]], on=self.id_emp, how="left")
        self.accepted_interval.rename(columns={"Start_Date":"Min_Date","End_Date":"Max_Date"}, inplace = True) # Rename the columns to avoid confusion with other merged columns
        print(self.master.columns)
        i=0
        for key, value in self.data.items():
            i += 1
            df = value
            ma = self.master
            ac = self.accepted_interval
            sql = "Select ac." + self.id_emp + ", ac.min_Date, ac.max_Date, df." + self.start_hist + ", df." + self.end_hist + " from ac, df on (df." + self.id_emp + "=" + "ac." + self.id_emp+ ");"
            sql2 = "Select ac.Personnel_number, ac.min_Date, ac.max_Date, ma." + self.start_hist + ", ma." + self.end_hist + " from ac, ma on (ma." + self.id_emp + "=" + "ac." + self.id_emp + \
                   ";)"
            result = ps.sqldf(sql, locals())
            print(key, sql)
            if i==1:
                self.accepted_interval = result

            else:
                self.accepted_interval = pd.concat[self.accepted_interval,result]

        print(self.accepted_interval.head(100))



"Select * from master m left join slave s on (m.Personnel_number = s.Personnel_number) where s.Start_Date>=m.Start_Date and m.End_Date>=s.End_Date;"




a = fusion(imported_files={"contrat_02": r"C:\Users\Sabri.GASMI\Desktop\Fusion\Old\CONTRAT_02.TXT",
                           "contrat_01": r"C:\Users\Sabri.GASMI\Desktop\Fusion\Old\CONTRAT_01.TXT"}, start_hist="Start Date",
           end_hist="End Date", id_employee="Personnel number")

a.data_preparation()
a.date_preparation()