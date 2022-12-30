import os
import yaml
from time import time
import click
import pickle
import multiprocessing
from gensim.models import Word2Vec
from gensim.models.phrases import Phrases, Phraser


@click.command()
@click.option('--config', default="training_config.yaml", help="yaml file with training configurations")
@click.option('--data', default="output/training_data.pkl", help="path to the training data pickle file")
@click.option('--output', default="output", help="output folder where the model will be saved")
def training(config, data, output):
    with open(config) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    print("Loading Training Data...")
    with open(data, 'rb') as filehandler:
        training_data = pickle.load(filehandler)

    print("Initializing...")
    phrases = Phrases(training_data, min_count=30, threshold=10000, progress_per=10000)
    bigram = Phraser(phrases)
    sentences = bigram[training_data]

    cores = multiprocessing.cpu_count()

    SocialVec_model = Word2Vec(min_count=20,
                               window=config['model_window'],
                               sg=config['model_sg'],
                               vector_size=config['vector_size'],
                               sample=1e-5,
                               alpha=0.03,
                               min_alpha=0.0007,
                               negative=config['negative_sampling'],
                               workers=cores - 1)

    print("Building vocabolary...")
    t = time()
    SocialVec_model.build_vocab(sentences, progress_per=10000)
    print('Time to build vocab: {} mins'.format(round((time() - t) / 60, 2)))

    print('Training model...')
    t = time()
    SocialVec_model.train(sentences, total_examples=SocialVec_model.corpus_count, epochs=20, report_delay=1)
    print('Time to train the model: {} mins'.format(round((time() - t) / 60, 2)))

    print('Saving model...')
    # save the model for later usage
    modelname_to_save = os.path.join(output, f"{model_prefix}.model")
    SocialVec_model.save(modelname_to_save)

    print("Done!")


if __name__ == "__main__":
    training()
