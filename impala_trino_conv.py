import pandas as pd 
import sqlparse
import re
import os




def files_to_be_updated():
    d1=pd.read_excel(r"path.xlsx")
    d2=d1.loc[d1['Initial Status '] =="ND"]
    l1=list(d2["Table validation"])
    return l1
    

# file location
def file_location(l1):
    search_directory = input("Enter path to start : ")
    
    items_path = []
    items =[]

    for foldername, subfolders, filenames in os.walk(search_directory):
        # Check filenames
        for filename in filenames:
            #print(filename),print(l1)
            if filename in [f'{l}.sql' for l in l1]:
            
                items_path.append(os.path.join(foldername, filename))
                items.append(filename)
            else:
                continue
            
    return items_path,items

# read the file
def read_file(i):
    file1 = open(f"{i}","r")
    file=file1.read()
    file1.close()
    return file

# update the file

def update(sql_code,filename,fol):
    
    
    fp=os.path.join(fol, filename)
    #print(fol)
    #print(filename)
    #print(fp)
    with open(f'{fp}', 'w') as f:
        f.writelines(sql_code)
      # to change file access modes

def change(sql_code):

    inital_transformations = {
    
    'dayofmonth':'day',
    'default.gb_format_datetime':'gb_format_datetime',
    'default.gb_json_parser':'gb_json_parser',
    'default.gb_to_est': 'gb_to_est',
    'default.gb_completed_months':'gb_completed_months',
    
    
    #'from_utc_timestamp':'at_timezone'
    
    #'float':'double'
    # Add more function mappings as needed
    }
    for impala_func, trino_func in inital_transformations.items():
        # Use re.sub with the defined replacements
        sql_code = re.sub(re.escape(impala_func), trino_func, sql_code, flags=re.IGNORECASE)

    def extract_functions(tree):
        res = []
        def visit(token):
            if token.is_group:
                for child in token.tokens:
                    visit(child)
            if isinstance(token, sqlparse.sql.Function):
                res.append(token.value)

        visit(tree)
        return set(res)


    


    set_1=extract_functions(sqlparse.parse(sql_code)[0])
    sorted_list = sorted(set_1, key=len, reverse =True)
    dict_1={}
    dict_2={}
    for i in range(len(sorted_list)):
        dict_1[f'zzz_{i}'] = sorted_list[i]
        dict_2[f'zzz_{i}'] = sorted_list[i]
        
    for i in range(len(sorted_list)):
        for j in range (i+1,len(sorted_list)):
            if dict_1[f'zzz_{j}'] in dict_1[f'zzz_{i}']:
                dict_2[f'zzz_{i}']=re.sub(re.escape(dict_1[f'zzz_{j}']), f'zzz_{j}', dict_2[f'zzz_{i}'], flags=re.IGNORECASE)
                
    impala=[]
    t=[]           
    for i in dict_1:

        impala.append(dict_1[i])


        #print(impala)
        modified = False  # Flag to track if modifications were made
            
            
            
        for match in re.finditer(r"(.*?)\((.*)\)", dict_2[i]):
            m1 = match.group(1)
            m2 = match.group(2)
            
            if m1.lower() == "datediff":
                
                    #arguments = [arg.strip() for arg in m2.split(',')]
                    #m3 = f"'day', cast({arguments[1]} as timestamp), cast({arguments[0]} as timestamp)"
                    #l = re.sub(r"(.*?)\((.*)\)", f'date_diff({m3})', i, flags=re.IGNORECASE)
                    #modified = True
                    match = re.match(r"(.*),(.*)", m2)
                    if match:
                        arg1 = match.group(1).strip()
                        arg2 = match.group(2).strip()
                        if 'zzz' in arg1:
                            arg1=dict_1[f'{arg1}']
                        else:
                            arg1
                        if 'zzz' in arg2:
                            arg2=dict_1[f'{arg2}']
                        else:
                            arg2 
                        m3 = f"'day', cast({arg2} as timestamp), cast({arg1} as timestamp)"
                        l = re.sub(r"(.*?)\((.*)\)",lambda match: f'date_diff({m3})',dict_2[i] , flags=re.IGNORECASE)
                        #print(l)


                    modified = True
                    #print(l)
                    t.append(l)
                    #print(t)
                    continue

                #"""
            if m1.lower() == "date_sub":

                    match = re.match(r"(.*),(.*)", m2)
                    if match:
                        arg1 = match.group(1).strip()
                        arg2 = match.group(2).strip()
                         
                        
                        if 'interval' in arg2 and 'months' in arg2:
                            match_1 = re.match(r"interval (.*) months", arg2)
                            arg3 = match_1.group(1).strip()
                            arg2 = re.sub(r"interval (.*) months",lambda match: f"{arg3}", arg2, flags=re.IGNORECASE)
                            m3 = f"'month', -{arg2}, cast({arg1} as timestamp)"
                            l = re.sub(r"(.*?)\((.*)\)",lambda match: f'date_add({m3})', dict_2[i], flags=re.IGNORECASE)
                        elif 'interval' in arg2 and 'day' in arg2:
                            match_1 = re.match(r"interval (.*) day", arg2)
                            arg3 = match_1.group(1).strip()
                            arg2 = re.sub(r"interval (.*) day",lambda match: f"{arg3}", arg2, flags=re.IGNORECASE)
                            m3 = f"'day', -{arg2}, cast({arg1} as timestamp)"
                            l = re.sub(r"(.*?)\((.*)\)",lambda match: f'date_add({m3})', dict_2[i], flags=re.IGNORECASE)
                        else:
                            m3 = f"'day', -{arg2}, cast({arg1} as timestamp)"
                            l = re.sub(r"(.*?)\((.*)\)",lambda match: f'date_add({m3})', dict_2[i], flags=re.IGNORECASE)
                        #l = re.sub(r"(.*?)\((.*)\)", f'date_add({m3})', l, flags=re.IGNORECASE)
                       


                    modified = True
                    #print(l)
                    t.append(l)
                    continue 
                #"""
            # regexp_replace -- date
            if m1.lower() == "regexp_replace":
                match = re.match(r"(.*),(.*),(.*)",m2)
                if match:
                    arg1 = match.group(1).strip()
                    arg2 = match.group(2).strip()
                    arg3 = match.group(3).strip()
                    
                    if "d{4}" in arg2 and  "d{2}" in arg2 and "1-" in arg3 and "2-" in arg3:
                        m3 = f"{arg1}"
                        l = re.sub(r"(.*?)\((.*)\)",lambda match: f'to_date(gb_format_datetime({m3}))', dict_2[i], flags=re.IGNORECASE)
                    else:
                        l=i

                    
                modified = True
                    #print(l)
                t.append(l)
                continue
                
                
                # trunc

            if m1.lower() == "trunc":
                match = re.match(r"(.*?),(.*)", m2)
                if match:
                    arg1 = match.group(1).strip()
                    arg2 = match.group(2).strip()
                     
                    m3 = f"{arg2}, cast({arg1} as timestamp)"
                    l = re.sub(r"(.*?)\((.*)\)",lambda match: f'date_trunc({m3})',dict_2[i], flags=re.IGNORECASE)


                modified = True
                    #print(l)
                t.append(l)
                continue

                # add_months

            if m1.lower() == "add_months":
                match = re.match(r"(.*),(.*)", m2)
                if match:
                    arg1 = match.group(1).strip()
                    arg2 = match.group(2).strip()
                     
                    m3 = f"'month', {arg2}, cast({arg1} as timestamp)"
                    l = re.sub(r"(.*?)\((.*)\)",lambda match: f'date_diff({m3})',dict_2[i], flags=re.IGNORECASE)


                modified = True
                    #print(l)
                t.append(l)
                continue


            if m1.lower() == "adddate":
                match = re.match(r"(.*),(.*)", m2)
                if match:
                    arg1 = match.group(1).strip()
                    arg2 = match.group(2).strip()
                     
                    m3 = f"'day', {arg2}, cast({arg1} as timestamp)"
                    l = re.sub(r"(.*?)\((.*)\)",lambda match: f'date_diff({m3})',dict_2[i], flags=re.IGNORECASE)


                modified = True
                    #print(l)
                t.append(l)
                continue

            if m1.lower() == "date_part":
                match = re.match(r"(.*?),(.*)", m2)
                if match:
                    arg1 = match.group(1).strip()
                    arg2 = match.group(2).strip()
                    
                    m3 = f"{arg1} from {arg2}"
                    l = re.sub(r"(.*?)\((.*)\)",lambda match: f'extract({m3})',dict_2[i], flags=re.IGNORECASE)


                modified = True
                    #print(l)
                t.append(l)
                continue   


                #left    
            if m1.lower() in ("left","strleft"):
                match = re.match(r"(.*),(.*)", m2)
                if match:
                    arg1 = match.group(1).strip()
                    arg2 = match.group(2).strip()
                     
                    m3 = f"'{arg1}', 1, {arg2}"
                    l = re.sub(r"(.*?)\((.*)\)",lambda match: f'substr({m3})',dict_2[i], flags=re.IGNORECASE)


                modified = True
                    #print(l)
                t.append(l)
                continue

                #right
            if m1.lower() in ("right","strright"):
                match = re.match(r"(.*),(.*)", m2)
                if match:
                    arg1 = match.group(1).strip()
                    arg2 = match.group(2).strip()
                    
                    m3 = f"'{arg1}',-{arg2}, {arg2}"
                    l = re.sub(r"(.*?)\((.*)\)",lambda match: f'substr({m3})',dict_2[i], flags=re.IGNORECASE)


                modified = True
                    #print(l)
                t.append(l)
                continue
              
                #from_utc_timestamp
            if m1.lower() == "from_utc_timestamp":
                    #print(m1)
                match = re.match(r"(.*),(.*)", m2)
                    
                if match:
                    arg1 = match.group(1).strip()
                    arg2 = match.group(2).strip()
                    
                    m3 = f"cast({arg1} as timestamp),{arg2}"
                        #print(m3),print(arg1),print(arg2)
                    l = re.sub(r"(.*?)\((.*)\)", lambda match: f'cast(at_timezone({m3}) as timestamp)',dict_2[i], flags=re.IGNORECASE)

                        #l = re.sub(r"(.*?)\((.*)\)", f'cast(at_timezone(timestamp {m3}) as timestamp)', i, flags=re.IGNORECASE)
                        #print(l)
                modified = True
                    #print(l)
                t.append(l)
                continue
                    
                #from_utc_timestamp
            if m1.lower() == "int_months_between":
                    #print(m1)
                match = re.match(r"(.*),(.*)", m2)
                    
                if match:
                    arg1 = match.group(1).strip()
                    arg2 = match.group(2).strip()
                    
                    m3 = f"'month',{arg1},{arg2}"
                        #print(m3),print(arg1),print(arg2)
                    l = re.sub(r"(.*?)\((.*)\)", lambda match: f'date_diff({m3})',dict_2[i], flags=re.IGNORECASE)

                        #l = re.sub(r"(.*?)\((.*)\)", f'cast(at_timezone(timestamp {m3}) as timestamp)', i, flags=re.IGNORECASE)
                        #print(l)
                modified = True
                    #print(l)
                t.append(l)
                continue 
                 
                   
            if m1.lower() == 'from_unixtime':
                    #print(i)
                match = re.match(r"(.*),(.*)", m2)
                if match:
                    arg1 = match.group(1).strip()
                    arg2 = match.group(2).strip()
                        #print(arg1),print(arg2)
                    
                    
                    #print(m3)
                date_change = {
                    'yyyy':'%Y',
                    'MM':'%m',
                    'dd':'%d',
                    'HH':'%H',
                    'mm':'%i',
                    'ss':'%s'
                     }


                for type_1, type_2 in date_change.items():
                    # Use re.sub with the defined replacements
                    arg2 = re.sub(re.escape(type_1), type_2, arg2, flags=re.IGNORECASE)
                m3 = f"{arg1}),{arg2}"
                l = re.sub(r"(.*?)\((.*)\)", f'date_format(from_unixtime({m3})',dict_2[i], flags=re.IGNORECASE)
                modified = True
                t.append(l)
                    
                continue
                    
            if m1.lower() == "date_add":

                match = re.match(r"(.*),(.*)", m2)
                if match:
                    arg1 = match.group(1).strip()
                    arg2 = match.group(2).strip()
                     
                    if 'interval' in arg2 and 'months' in arg2:
                        match_1 = re.match(r"interval (.*) months", arg2)
                        arg3 = match_1.group(1).strip()
                        arg2 = re.sub(r"interval (.*) months",lambda match: f"{arg3}", arg2, flags=re.IGNORECASE)
                        m3 = f"'month', {arg2}, cast({arg1} as timestamp)"
                        l = re.sub(r"(.*?)\((.*)\)",lambda match: f'date_add({m3})',dict_2[i], flags=re.IGNORECASE)
                    elif 'interval' in arg2 and 'day' in arg2:
                        match_1 = re.match(r"interval (.*) day", arg2)
                        arg3 = match_1.group(1).strip()
                        arg2 = re.sub(r"interval (.*) day",lambda match: f"{arg3}", arg2, flags=re.IGNORECASE)
                        m3 = f"'day', {arg2}, cast({arg1} as timestamp)"
                        l = re.sub(r"(.*?)\((.*)\)",lambda match: f'date_add({m3})',dict_2[i], flags=re.IGNORECASE)
                    else:
                        m3 = f"'day', {arg2}, cast({arg1} as timestamp)"
                        l = re.sub(r"(.*?)\((.*)\)",lambda match: f'date_add({m3})',dict_2[i], flags=re.IGNORECASE)
                        
                        #l = re.sub(r"(.*?)\((.*)\)", f'date_add({m3})', l, flags=re.IGNORECASE)



                modified = True
                    #print(l)
                t.append(l)
                continue   
                    


    #select from_unixtime(UNIX_TIMESTAMP(now()),'yyyy-MM-dd'); -> select date_format(from_unixtime(to_unixtime(now())),'%Y-%m-%d');

        if not modified:

            t.append(dict_1[i])
    for original, modified in reversed(list(dict_1.items())):
        t = [i.replace(original, modified) for i in t]
    zipped_dict = dict(zip(impala, t))
    sorted_dict = dict(sorted(zipped_dict.items(), key=lambda item: len(item[0]) ,reverse =True))
    #print(sorted_dict)

    for original, modified in sorted_dict.items():
        sql_code = sql_code.replace(original, modified)


    def replace_interval(match):
        value = match.group(1)
        unit = match.group(2)

        # Normalize units
        if unit.lower().startswith('day'):
            unit = 'day'
        elif unit.lower().startswith('month'):
            unit = 'month'
        elif unit.lower().startswith('hour'):
            unit = 'hour'
        elif unit.lower().startswith('week'):
            unit = 'week'
        elif unit.lower().startswith('minute'):
            unit = 'minute'
        elif unit.lower().startswith('second'):
            unit = 'second'

        return f'interval \'{value}\' {unit}'

    modified_query = re.sub(r'\binterval\s+(\d+)\s*(day|days|month|months|hour|hours|week|weeks|minute|minutes|second|seconds)\b', replace_interval, sql_code, flags=re.IGNORECASE)

    function_transformations = {
        'IFNULL': 'coalesce',
        'CURRENT_TIMESTAMP()': 'current_timestamp',

        r'\bSTRING': 'varchar',
        r'\bSTRING)': 'varchar)',
        'string)':'varchar)',
        r'\bINT\b': 'integer',  # Using \b for word boundary
        r'\bfloat\b': 'double',# Using \b for word boundary
        'int)':'integer)',
        'float)':'double)',
        #r'\bEST\b': 'EST5EDT',
        #r'EST)': 'EST5EDT)',
        #r'\bEDT\b': 'EST5EDT',
        #r'EDT)': 'EST5EDT)',
        "'EST'":"'EST5EDT'",
        'IFNULL': 'coalesce',
        "'EDT'":"'EST5EDT'",
        '`' :'"',
        'unix_timestamp':'to_unixtime',
        'dayofmonth':'day',
        'locate':'strpos',
        'unix_timestamp':'to_unixtime',
        'today':'current_date',
        'instr':'strpos',
        #'float':'double'
        # Add more function mappings as needed
    }


    for impala_func, trino_func in function_transformations.items():
        # Use re.sub with the defined replacements
        modified_query = re.sub(re.escape(impala_func), trino_func, modified_query, flags=re.IGNORECASE)


    return modified_query    


def update_to_trino():
    file_list =files_to_be_updated()
    if len(file_list)>0:
        items_path,items= file_location(file_list)
        print(items_path),print(len(items))
        dic={}
        for a in range(len(items)):
            dic[items[a]] = items_path[a]
        print(dic)
        if len(items_path) >0 and len(items) > 0:
            dic_1={}
            for items, items_path in dic.items():
                print(items),print(items_path)
                print(f'Reading file {items_path}')
                file=read_file(items_path)
                print(f'{file}')
                trino_code=change(file)
                print(f'Trino code -> {trino_code}')
                dic_1[f'{items}'] = f'{trino_code}'
        else:
            print('Items might not be available')
        fol = input("Enter the folder path : ")            
        for items,trino_code in dic_1.items():
            update(trino_code,items,fol)
            print(f'Updating {items}')
            
              
    else:
        print('All are marked Done')
        
    



def main():
    update_to_trino()
    """
    parser = argparse.ArgumentParser(description = ‘Parse command line options’)
    
    
    parser.add_argument(‘-f’, help=‘input and output file name’, nargs=1)
    args = parser.parse_args()
    if not args.f :
        print (parser.print_usage())
    else :
        print (“----------------------“)
        cur_impala,con_impala = impala_connection()
        insert_table(args.f[0],cur_impala,con_impala)
        #readFile(args.f[0],args.f[1])

    """
if __name__ == "__main__":
    main()
