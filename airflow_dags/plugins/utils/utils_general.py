import pandas as pd

def create_interaction_matrix(data: pd.DataFrame, target_cols: list, id_to_acc: dict):
    
    data['max_date'] = data['div_data'].max()
    data['days_from_max_date'] = (data['max_date'] - data['div_data']).dt.days
    data['days_from_max_date'] = data['days_from_max_date'].astype(int)
    data['weight'] = data['days_from_max_date'] / data['days_from_max_date'].max()
    
    for target in target_cols:
        data[target] = data[target] * data['weight']

    interaction_matrix = data[['client_id'] + target_cols]
    interaction_matrix = interaction_matrix.groupby('client_id').sum().reset_index()
    
    interaction_matrix['accounts'] = [[targets] for targets in interaction_matrix[target_cols].values]
    interaction_matrix['accounts'] = interaction_matrix['accounts'].apply(lambda x: x[0])
    interaction_matrix['accounts_name'] = [list(id_to_acc.keys())] * interaction_matrix.shape[0]
    
    interaction_matrix = interaction_matrix[['client_id', 'accounts', 'accounts_name']]
    
    interaction_matrix_exploded = list()
    interaction_matrix = interaction_matrix.values

    for row in interaction_matrix:
        accounts = row[1]
        accounts_name = row[2]
        client_id = row[0]
        for account, account_name in zip(accounts, accounts_name):
            interaction_matrix_exploded.append((client_id, account, account_name))

    interaction_matrix_exploded = pd.DataFrame(interaction_matrix_exploded, columns=['client_id', 'account', 'account_name'])
    interaction_matrix_exploded = interaction_matrix_exploded[interaction_matrix_exploded['account'] > 0]

    
    return interaction_matrix_exploded


def create_interaction_matrix_target(data: pd.DataFrame, target_cols: list, id_to_acc: dict):

    interaction_matrix = data[['client_id'] + target_cols]
    interaction_matrix = interaction_matrix.groupby('client_id').sum().reset_index()
    
    interaction_matrix['accounts'] = [[targets] for targets in interaction_matrix[target_cols].values]
    interaction_matrix['accounts'] = interaction_matrix['accounts'].apply(lambda x: x[0])
    interaction_matrix['accounts_name'] = [list(id_to_acc.keys())] * interaction_matrix.shape[0]
    
    interaction_matrix = interaction_matrix[['client_id', 'accounts', 'accounts_name']]
    
    interaction_matrix_exploded = list()
    interaction_matrix = interaction_matrix.values

    for row in interaction_matrix:
        accounts = row[1]
        accounts_name = row[2]
        client_id = row[0]
        for account, account_name in zip(accounts, accounts_name):
            interaction_matrix_exploded.append((client_id, account, account_name))

    interaction_matrix_exploded = pd.DataFrame(interaction_matrix_exploded, columns=['client_id', 'account', 'account_name'])
    interaction_matrix_exploded = interaction_matrix_exploded.rename(columns={'account': 'target'})
    interaction_matrix_exploded['target'] = (interaction_matrix_exploded['target'] > 0).astype(int)
    # interaction_matrix_exploded = interaction_matrix_exploded[interaction_matrix_exploded['account'] > 0]

    
    return interaction_matrix_exploded