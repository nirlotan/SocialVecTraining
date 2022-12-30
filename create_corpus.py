import os
import click
import pickle
import pandas as pd

@click.command()
@click.option('--input', required=True, help='Path to collected users file')
@click.option('--popular_count', default=300, help='Number of appearences of an entity in order for it to be accounted as popular')
@click.option('--output', default='output', help='Path to the location where the corpus will be saved')
def create_corpus(input, popular_count, output):
    df = pd.read_csv(input, names=['user','follows'])

    # Count the number of occurrences of the followed users in order to identify popular users
    df['user'] = df['user'].astype(int)
    df['follows'] = df['follows'].astype(int)
    count_df = df.groupby('follows').size().to_frame('sum')
    df_popular = count_df[count_df['sum'] > popular_count]
    popular_list = df_popular.index.tolist()

    subset_df_popular = df[df['follows'].isin(popular_list)].copy()
    subset_df_popular['follows'] = subset_df_popular['follows'].astype(str)
    train_data = [item for item in subset_df_popular.groupby('user')['follows'].apply(list).reset_index(name='new')['new']]

    # Check if an output folder exists
    if not os.path.exists(output):
        # Create the folder
        os.makedirs(output)

    output_filename = os.path.join(output, 'training_data.pkl')
    with open(output_filename, 'wb') as filehandler:
        pickle.dump(train_data, filehandler)

if __name__ == '__main__':
    create_corpus()