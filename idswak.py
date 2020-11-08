# @file idswak.py
# @brief  Tool to manage IDs in datasets
# GLOBAL USAGE: 
#    support interactive mode
# AUTHOR: WuLung Hsu, wuulong@gmail.com
# CREATE DATE: 2020/11/2

# standard
import os.path
import sys
import os
from datetime import datetime

#extend
import pandas as pd 
import pandasql as ps

#const
FILE_IDACTCFG="include/id_act_cfg.csv"
FILE_IDACTFUSION_SRC="dataset/id_act_fusion.csv"
FILE_IDACTFUSION="output/id_act_fusion.csv"
FILE_IDACTLOG="output/id_act.log"
FILE_IDACTRPT="output/id_act.rpg"

APP_NAME="idswak"
VER="0.1"

def get_value_by_index(df,keyvalue, target_col):
    """
    find df's column(key) = value, return value of target_col
    keyvalue: col_name=value
    """
    cols = keyvalue.split("=")
    if len(cols)!=2:
        return ""
    keyvalue_key = cols[0]
    keyvalue_value = cols[1]
    if not target_col in df.columns:
        return ""
    values = df[df[keyvalue_key]==keyvalue_value][target_col].values.tolist()
    if len(values)>0:
        value = values[0]
    else:
        value = ""
    return value

class IdSwak():
    """
        Tool to manage IDs in datasets
        HINT: may not need to be here. current can be standalone
    """
    def __init__(self):
        print("%s-V%s" %(APP_NAME,VER))
        self.df_cfg = None 
        self.cfg = {} # ds_name->{} , [col_name]=value
         
        self.dfs = {} #ds_name->ds
        self.name_used = {}  #名稱在哪些筆資料中使用 # ->[fid,...] 
        
        self.df_fusion = None
        self.fids = {} #fid->fusion record
            #fusion record [fid_master,name,fid_link,guess_link,content]
        self.load_cfg() 
    
    def load_cfg(self):
        """
            load cfg, df_cfg, df_fusion
            reload to refresh config
        """
        self.df_cfg = pd.read_csv(FILE_IDACTCFG)
        for idx, row in self.df_cfg.iterrows():
            #if row['enabled']==1:
            self.cfg[row['ds_name']] = row
        
        if not os.path.isfile(FILE_IDACTFUSION):
            if os.path.isfile(FILE_IDACTFUSION_SRC):
                self.df_fusion = pd.read_csv(FILE_IDACTFUSION_SRC)
        else:     
            self.df_fusion = pd.read_csv(FILE_IDACTFUSION)
        self.load_fusion()
    def load_ds(self,ds_name):
        #filename = get_value_by_index(self.df_cfg, "ds_name=%s" %(ds_name), "filename")
        filename = self.cfg[ds_name]['filename']
        df = pd.read_csv(filename)
        if self.cfg[ds_name]['id_type']=="wikidata":
            df['item'] = df['item'].str.replace('http://www.wikidata.org/entity/','')
            #df['item'].replace({"http://www.wikidata.org/entity/": ""}, inplace=True)
        
        #drop_col_name = 'Unnamed: 0'
        #if drop_col_name in df.columns:
        #    df=df.drop([], axis=1)
        return df
    def load_ds_all(self):
        if len(self.dfs)>0: #already load
            return 
        for ds_name in self.cfg.keys():
            if self.cfg[ds_name]['enabled']==1:
                self.dfs[ds_name]= self.load_ds(ds_name)
    def get_dsname_by_src(self,src_id):
        for cfg in self.cfg.keys():
            if self.cfg[cfg]['src_id']==src_id:
                return self.cfg[cfg]['ds_name']
        return ''
    def series_remove_nan(self,series): 
        nan_elems = series.isnull()
        remove_nan = series[~nan_elems]  
        return  remove_nan

    def wd_url_to_wid(self,url):
        #transfer wikidata url to wid
        #http://www.wikidata.org/entity/Q11038144
        return url.replace("http://www.wikidata.org/entity/","")

    def fusion_add_pair(self,override, fid,name,fid_link='',guess_link=''):
        """
            update one fusion record
            override: False, True
            同一個 DS 同名不會融合
            cases:
                1. setup record
                2. first record
                3. same name record
                4. same name with same src_id
            
            [FIXME] too complex
        """
        if pd.isna(fid_link):
            fid_link = ''
        if pd.isna(guess_link):
            guess_link = ''
        record = [fid, name, fid_link,'','']
        if override: # setup new record
            self.fids[fid] = record
        else:
            #find if have old record
            cols = fid.split("@")
            src_id=""
            if len(cols)>0:
                src_id = cols[0]
            exist = False
            if name in self.name_used:
                for fid_tmp in self.name_used[name]:
                    if fid_tmp in self.fids:
                        if fid_tmp!=fid and fid_tmp.find(src_id)>=0: #同名不同 src_id 才行
                            pass
                        else:
                            fid_record = fid_tmp
                            record = self.fids[fid_record]
                            exist=True
                        break
                    else:
                        exist=False
            
            if exist==False:
                if fid in self.fids:
                    record = self.fids[fid]
                    if record[3] =='':
                        #record[3] = fid
                        pass
                        
                    else:
                        record[3] = "%s|%s" %(record[3], fid)
                    
                else:
                    record = [fid, name, '','','']
                self.fids[fid] = record # fid_master, name, fid_link, guess_link 
            else:
                if record[0].find(fid)<0:
                    if record[3] =='':
                        record[3] = fid
                    else:
                        #remove already in record
                        if record[3].find(fid)<0:
                            record[3] = "%s|%s" %(record[3], fid)
                #update
                
                    self.fids[fid_record] = record # fid_master, name, fid_link, guess_link 

            
        
    def load_fusion(self): #load by default
        """
            load fusion confirm status from file.
        """
        if self.df_fusion is not None:
            for idx, row in self.df_fusion.iterrows():
                self.fusion_add_pair(True,row['fid_master'],row['name']  , row['fid_link'], row['guess_link'])
    def init_fusion(self): #need to run after df load
        """
            update fusion status from data
        """
        #df should already loaded
        for ds_name in self.cfg.keys():
            if self.cfg[ds_name]['enabled']==1:
                col_id = self.cfg[ds_name]['col_id']
                src_id = self.cfg[ds_name]['src_id']
                col_name = self.cfg[ds_name]['col_name']
                id_type = self.cfg[ds_name]['id_type']
                df = self.dfs[ds_name]
                for idx, row in df.iterrows():
                    col2_id = row[col_id]
                    #if id_type=='wikidata':
                    #    col2_id=self.wd_url_to_wid(row[col_id])
                    fid = "%s@%s" %(src_id, col2_id)
                    self.fusion_add_pair(False,fid, row[col_name])
    def output_fusion(self):
        """
            save current fusion to file
        """
        #backup fusion file
        if os.path.isfile(FILE_IDACTFUSION):
            
            now = datetime.now() # current date and time
            date_time = now.strftime("%Y%m%d_%H%M%S")
            filename_time = "output/id_act_fusion_%s.csv" %(date_time)
            os.rename(FILE_IDACTFUSION,filename_time)
        with open("output/id_act_fusion.csv", "w") as outfile:
            outfile.write("fid_master,name,fid_link,guess_link,content\n") 
            
            for fid  in self.fids.keys():
                #print("%s" %(self.fids[fid]))
                fid_strlist = map(str,self.fids[fid])
                outfile.write(",".join(fid_strlist))
                outfile.write("\n")
    
    def fusion_act(self,act_cmd):
        """
            apply some action to fusion result
            info: src record count
            content: generate content field
        """        
        if act_cmd=="info": # provide information
            cnt = len(self.fids)
            src_cnt={}
            for fid  in self.fids.keys():
                cols = fid.split("@")
                src_id = cols[0]
                if src_id in src_cnt:
                    src_cnt[src_id]+=1
                else:
                    src_cnt[src_id]=1
            print("src_id counter:")
            src_info=[]
            for src_id in src_cnt:
                src_info.append([src_id,src_cnt[src_id]])
                #print("%s,%i" %(src_id,src_cnt[src_id]))
            return pd.DataFrame(src_info)
        if act_cmd=="content": #update content csv to self.fids, colname without src_id
            for fid_master  in self.fids.keys(): #per fusion record
                #print("fid_master=%s" %(fid_master))
                cols_master = fid_master.split("@")
                src_id_master = cols_master[0]
                record = self.fids[fid_master]
                fid_list=[]
                fid_list.append(fid_master)
                fid_link = record[2] 
                if not fid_link=='':
                    #print("fid_link=%s" %(fid_link))
                    fid_links = fid_link.split("|")
                    fid_list.extend(fid_links)
                guess_link = record[3]
                if not guess_link=='':
                    #print("guess_link=%s" %(guess_link))
                    guess_links = guess_link.split("|")
                    fid_list.extend(guess_links)
                
                col_strs = [] #content strs
                for fid in fid_list: #per fusion fid
                    cols = fid.split("@")
                    src_id = cols[0]
                    id = cols[1]
                    ds_name = self.get_dsname_by_src(src_id)
                    if ds_name=='':
                        print("ds_name empty, src_id=%s" %(src_id))
                    else:
                        col_id = self.cfg[ds_name]['col_id']
                        df = self.dfs[ds_name]
                        #print("col_id=%s,id=%s" %(col_id,id))
                        try:
                            if pd.api.types.is_integer_dtype(df.dtypes[col_id]):
                                id= int(id)
                            df_row = df[df[col_id]==id]
                            for idx, row in df_row.iterrows():
                                for col_name in df.columns:
                                    if col_name=='Unnamed: 0':
                                        pass
                                    else:
                                        col_str = "%s=%s" %(col_name, row[col_name])
                                        col_strs.append(col_str)
                        except:
                            print("exception! fid_master=%s, fid_link=%s, guess_link=%s, col_id=%s" %(fid_master,fid_link,guess_link,col_id))
                record[4] = "\"%s\"" %( "|".join(col_strs))
                #print("content=%s" %(record[4]))
                self.fids[fid_master] = record
                    
                    
                
                
  
    def prepare_by_scan(self, scan_cmd):
        """
        prepare name_used, output idact_fid_name.csv
        scan_cmd: '' - default
        FIXME: old code with load df.
        """
        name_used = {} #名稱在哪些 src_id 中使用 # ->[src_id,...] 
        lines = []
        #show all record
        for idx, row in self.df_cfg.iterrows():
            if row['enabled']!=1:
                continue
            ds_name = row['ds_name']
            id_type = row['id_type']
            col_id = row['col_id']
            col_name = row['col_name']
            col_key = row['col_key']
            src_id = row['src_id']
            #print("----- DS_NAME:%s -----" %(ds_name))
            df = self.load_ds(ds_name)
            #print("key=%s" %(col_key))
            if not pd.isnull(col_key):
            #if col_key !="":
                keys = col_key.split(",")
                df.sort_values(by=keys)
            
            
            for idx2, row2 in df.iterrows():
                format_str = "%s@%s,%s"
                value = row2[col_name]
                col2_id = row2[col_id]
                
                if id_type=='wikidata':
                    wid=self.wd_url_to_wid(col2_id)
                    id_str = format_str %(src_id,wid,value)
                    fid = "%s@%s" %(src_id,wid)
                    
                else:
                    id_str = format_str %(src_id,col2_id,value)
                    fid = "%s@%s" %(src_id,col2_id)
                #print(id_str) 
                lines.append(id_str)
                if value in name_used:
                    if not fid in name_used[value] :
                        name_used[value].append(fid)
                else:
                    name_used[value] = [ fid ]
                
            
        self.name_used = name_used
        with open("output/idact_fid_name.csv", "w") as outfile:
            outfile.write("fid,name\n") 
            outfile.write("\n".join(lines))       

    def ds_gen_id(self,ds_name,act_cmd):
        """
        generate ds ID by act_cmd
        """
        row_cfg = self.df_cfg[self.df_cfg['ds_name']==ds_name]
        col_id = row_cfg['col_id'].values[0]
        col_name = row_cfg['col_name'].values[0]
        col_key = row_cfg['col_key'].values[0]
        
        df = self.load_ds(ds_name)
        if col_key !="":
            keys = col_key.split(",")
            df.sort_values(by=keys)
            
        lst = df[col_name].values.tolist()
        if act_cmd=='N':
            #new
            format_str = "N|%i||%s"
            for i in range(len(lst)):
                id_str = format_str %(i+1,lst[i])
                print(id_str)
        if act_cmd=='M': #insert missing
            pass
        
    def col_action(self,df_m, colname_m, df_s, colname_s, act_cmd):
        """
        2 col action, current have compare feature
        act_cmd: 
            "": only compare
            "o" : output
        """
        if not colname_m in df_m.columns:
            return 
        if not colname_s in df_s.columns:
            return 

        vs_m = set(self.series_remove_nan(df_m[colname_m]).unique())
        vs_s = set(self.series_remove_nan(df_s[colname_s]).unique())
        
        out = [vs_m.union(vs_s),vs_m.intersection(vs_s),vs_m.difference(vs_s),vs_s.difference(vs_m)]
        
        print("counter of A=%i, B=%i, A+B=%i,A&B=%i,A-B=%i,B-A=%i\n" %(len(vs_m),len(vs_s),len(out[0]),len(out[1]),len(out[2]),len(out[3])))
        print("A+B=%s\n\nA&B=%s\n\nA-B=%s\n\nB-A=%s\n\n" %(out[0],out[1],out[2],out[3]))
        
        if act_cmd=="o":
            with open("output/idact_A+B.csv", "w") as outfile:
                outfile.write("\n".join(out[0]))
            with open("output/idact_A&B.csv", "w") as outfile:
                outfile.write("\n".join(out[1]))
            with open("output/idact_A-B.csv", "w") as outfile:
                outfile.write("\n".join(out[2]))
            with open("output/idact_B-A.csv", "w") as outfile:
                outfile.write("\n".join(out[3]))
        
        #a.intersection(b)
        #a.symmetric_difference(b)
    def col_action_str(self,act_str):
        """
            2 column action trigger by setting string
            act_str = "ds_a,colname_a,ds_b,colname_b,act_cmd"
        """
        cols = act_str.split(",")
        if len(cols)!=5:
            return 
        df_m = self.load_ds(cols[0])
        df_s = self.load_ds(cols[2])
        if ( df_m is None or df_s is None) :
            return 
        self.col_action(df_m,cols[1],df_s,cols[3],cols[4])
        
        
    def desc_ds_col(self,ds_name,col_name): #col_name: real name
        """
            describe one column
        """
        df=self.load_ds(ds_name)
        #df
        #ds_name="排水"
        #col_id = get_value_by_index(idMgr.df_cfg,"ds_name=%s" %(ds_name), "col_id")
        #print(col_id)
        #col_id='Drain_Name'
        lst = df[col_name].values.tolist()
        lst_unique = df[col_name].unique()
        print("ds_name=%s, count=%i \ncol=%s,unique=%i, duplicated=%i" %(ds_name,len(lst),col_name, len(lst_unique), len(lst)- len(lst_unique))) 
        #unique
        print("unique=\n%s" %(df[col_name].unique()))
        #duplicate
        idx_d = df[col_name].duplicated() 
        dup_values = df[col_name][idx_d].values
        dup_set = set(dup_values) #unique
        print( "duplicate distinct cnt = %i, values=\n%s" % (len(dup_set),dup_set))
        
    def desc_ds(self,df, ds_name):
        """
            describe one dataset
        """
        desc_str = "shape=%s" % (df.shape)
        col_id = get_value_by_index(self.df_cfg,"ds_name=%s" %(ds_name), "col_id")
        if col_id=="":
            print("Can't get %s's col_id")
            return
        col_name = get_value_by_index(self.df_cfg,"ds_name=%s" %(ds_name), "col_name")
        if col_name=="":
            print("Can't get %s's col_name")
            return 
    def ds_name_merge(self,ds_names):
        """
            union names in giving datasets
        """
        #ds_names = [ds_name,...]
        self.load_ds_all()
        names = []
        for ds_name in ds_names:
            col_name = self.cfg[ds_name]['col_name']
            names.extend(self.series_remove_nan(self.dfs[ds_name][col_name]).values.tolist())
        names_u = list(set(names))
        return names_u
    def find_name_info(self,find_name):
        """
        find all record of find_name
        """
        for idx, row in self.df_cfg.iterrows():
            if row['enabled']!=1:
                continue
            ds_name = row['ds_name']
            id_type = row['id_type']
            col_id = row['col_id']
            col_name = row['col_name']
            col_key = row['col_key']
            src_id = row['src_id']
            #print("----- DS_NAME:%s -----" %(ds_name))
            df = self.load_ds(ds_name)
            #print("key=%s" %(col_key))
            if not pd.isnull(col_key):
            #if col_key !="":
                keys = col_key.split(",")
                df.sort_values(by=keys)
            
            df2 = df[df[col_name]==find_name]
            for idx2, row2 in df2.iterrows():
                format_str = "\n%s@%s,%s"
                value = row2[col_name]
                if id_type=='wikidata':
                    wid=self.wd_url_to_wid(row2[col_id])
                    id_str = format_str %(src_id,wid,value)
                    
                else:
                    id_str = format_str %(src_id,row2[col_id],value)
                print(id_str) 
                print("%s" %(row2))

if sys.argv[0] == '':
    #interactive
    idMgr = IdSwak()
    print("idMgr inited for interactive used")

