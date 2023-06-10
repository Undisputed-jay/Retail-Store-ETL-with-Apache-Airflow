def data_cleaner():
    
    import pandas as pd
    import re
    
    df = pd.read_csv('/opt/airflow/data/raw_store_transactions.csv')
    
    # writing a function to remove non-word characters(\w) and whitespaces(\s) and strip them off with an empty space.
    def clean_location(a):
        return re.sub(r'[^\w\s]', '', a).strip()
    
    
    # writing a function to find all digits and and using if function to return the first occurrence 
    def clean_product_id(b):
        matches = re.findall(r'\d+', b)
        if matches:
            return matches[0]
        else:
            return b
     
     
    #writing a function to repace the dollar sign with nothing 
    def remove_dollar(amount):
        return float(amount.replace('$', ''))
    
    
    # Applying the clean_location function to the STORE_LOCATION column
    df['STORE_LOCATION'] = df['STORE_LOCATION'].apply(lambda x: clean_location(x))
    
    # Applying the clean_product_id function to the PRODUCT_ID column
    df['PRODUCT_ID'] = df['PRODUCT_ID'].apply(lambda y: clean_product_id(y))
    
    
    # Applying the remove dollar function to the 4 columns
    for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].apply(lambda z: remove_dollar(z))
        
    df.to_csv('/opt/airflow/data/clean_store_transactions.csv', index= False)
    
    
    
