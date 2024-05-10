# bank_reconciliation_v8.py 
import os 
import pandas as pd
from fuzzywuzzy import process
pd.options.display.max_rows = None
pd.options.display.max_columns = None

with open('Recurring matched amounts.csv', 'w') as f:
    f.write('Trans #, Type, Date_cash, Num, Name, Memo, Amount, Catalog_Index, Date_bank, Description, Debit, Credit, Balance, Score\n')

def find_best_matches(df):
    best_matches = [] 
    for index, row in df.iterrows():
        # Find the best match for the current row in the DataFrame Bank['Description'] with the list of Memo
        match, score = process.extractOne(row['Description'], [row2['Memo'] for index2, row2 in df.iterrows()])
        best_matches.append((match, score, [row2['Memo'] for index2, row2 in df.iterrows()].index(match)))
         
    # Add two columns to df: 'Score' and 'Match_index'
    df['Score'] = [item[1] for item in best_matches]
    df['Match_index'] = [item[2] for item in best_matches]

    # split the df into two DataFrames: df_cash and df_bank
    df_cash = df.iloc[:, :8] 
    df_bank = df.iloc[:, 8:] 

    # rename the columns of df_cash['Catalog_Index'] to df_cash['Match_index']  
    df_cash = df_cash.rename(columns={'Catalog_Index': 'Match_index'}) 

    # merge the two DataFrames on 'Match_index'
    df = pd.merge(df_cash, df_bank, how='inner', on=['Match_index']) 
    # write df to a new csv file 
    df.to_csv("Recurring matched amounts.csv", header=False, index=False, mode='a') 
    return df

 
def reconcile_bank_transactions(input_file, output_file):
    # Check if the file exists and close it if it is open
    try:
        if os.path.isfile(output_file):
            os.remove(output_file)  # Remove the file if it exists
    except Exception as e:
        print("Error while removing existing file:", e)
    # Read the data from both files
    cash_transactions_df = pd.read_excel(input_file + '_cash.xlsx')
    bank_transactions_df = pd.read_excel(input_file + '_bank.xlsx')
    # Clean and preprocess the data if necessary
    # For example, convert dates to datetime format
    cash_transactions_df['Date'] = pd.to_datetime(cash_transactions_df['Date'])
    bank_transactions_df['Date'] = pd.to_datetime(bank_transactions_df['Date'])
    # Convert the amounts to floats
    cash_transactions_df['Amount'] = cash_transactions_df['Amount'].astype(float)
    bank_transactions_df['Amount'] = bank_transactions_df['Amount'].astype(float)
    # Add a unique catalog_index to each transaction on both files
    cash_transactions_df['Catalog_Index'] = cash_transactions_df.groupby('Amount').cumcount()  
    bank_transactions_df['Catalog_Index'] = bank_transactions_df.groupby('Amount').cumcount()  
    # Sort the DataFrames by 'Amount' and then by 'Catalog_Index'
    cash_transactions_df.sort_values(by=['Amount', 'Catalog_Index'], inplace=True)
    bank_transactions_df.sort_values(by=['Amount', 'Catalog_Index'], inplace=True)
    # Merge the sorted DataFrames on 'Amount' and 'Catalog_Index'
    matched_transactions = pd.merge(cash_transactions_df, bank_transactions_df, how='inner', on=['Amount', 'Catalog_Index'], suffixes=('_cash', '_bank'))
    # Modify the date format in the merged report
    matched_transactions['Date_cash'] = matched_transactions['Date_cash'].dt.strftime('%Y-%m-%d')
    matched_transactions['Date_bank'] = matched_transactions['Date_bank'].dt.strftime('%Y-%m-%d')
 
    # unique_amounts  
    unique_amounts = matched_transactions.groupby('Amount').filter(lambda x: len(x) == 1)

    # repeated_amounts = matched_transactions[matched_transactions['Amount'].duplicated(keep=False)] 
    repeated_amounts = matched_transactions.groupby('Amount').filter(lambda x: len(x) > 1) 
    grouped_repeated_amounts = repeated_amounts.groupby('Amount')   

    # Identify unmatched transactions
    unmatched_cash_transactions = cash_transactions_df[~cash_transactions_df.set_index(['Amount', 'Catalog_Index']).index.isin(matched_transactions.set_index(['Amount', 'Catalog_Index']).index)]
    unmatched_bank_transactions = bank_transactions_df[~bank_transactions_df.set_index(['Amount', 'Catalog_Index']).index.isin(matched_transactions.set_index(['Amount', 'Catalog_Index']).index)]
 
    # if unmatched_cash_transactions or unmatched_bank_transactions:
    if len(unmatched_cash_transactions) !=0:
        print(f"Unmatched cash transactions: {len(unmatched_cash_transactions)}")
         
    elif len(unmatched_bank_transactions)!=0:
        print(f"Unmatched bank transactions:{len(unmatched_bank_transactions)}")
        
    else:
        print("No unmatched transactions found. Congratulations!")   
 
    # Export the detailed report to a csv file 
    unique_amounts.to_csv(output_file, header=True, index=False, mode='w')
    if len(unmatched_cash_transactions) != 0: 
        unmatched_cash_transactions.to_csv(output_file, header=True, index=False, mode='a')
    if len(unmatched_bank_transactions) != 0:
        unmatched_bank_transactions.to_csv(output_file, header=True, index=False, mode='a')

    for name, group in grouped_repeated_amounts:
        find_best_matches(group) 
  
def main():
    input_file = '2023'
    output_file = 'Unique matched amounts.csv'
    reconcile_bank_transactions(input_file, output_file)

if __name__ == '__main__':
    main()

