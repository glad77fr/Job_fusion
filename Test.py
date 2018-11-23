for key, value in self.data.items():
    i += 1
    df = value
    ma = self.master
    ac = self.accepted_interval
    sql = "Select ac." + self.id_emp + ", ac.min_Date, ac.max_Date, df." + self.start_hist + ", df." + self.end_hist + " from ac, df on (df." + self.id_emp + "=" + "ac." + self.id_emp + ");"
    sql2 = "Select ac.Personnel_number, ac.min_Date, ac.max_Date, ma." + self.start_hist + ", ma." + self.end_hist + " from ac, ma on (ma." + self.id_emp + "=" + "ac." + self.id_emp + \
           ";)"
    result = ps.sqldf(sql, locals())
    print(key, sql)
    if i == 1:
        self.accepted_interval = result

    else:
        print(result.head(50))
        accepted_interval = [self.accepted_interval, result]
        self.accepted_interval = pd.concat(accepted_interval, ignore_index=True)