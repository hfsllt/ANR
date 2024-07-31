import pickle

def write_cache(cached_openalex_data,path):
    pickle.dump(cached_openalex_data, open(path, 'wb'))

def load_cache(cached_openalex_data,path):
    cached_openalex_data = pickle.load(open(path, 'rb'))
    print(f'{len(cached_openalex_data)} data in cache')
    return cached_openalex_data