import tweepy
import json
import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener las credenciales desde las variables de entorno
api_key = os.getenv('API_KEY')
api_secret_key = os.getenv('API_SECRET_KEY')
access_token = os.getenv('ACCESS_TOKEN')
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')
bearer_token = os.getenv('BEARER_TOKEN')

# Autenticación con Tweepy (usando OAuth 1.0a)
auth = tweepy.OAuthHandler(api_key, api_secret_key)
auth.set_access_token(access_token, access_token_secret)

# Conectar con la API de Twitter
api = tweepy.API(auth)

# Función para buscar tweets recientes con un hashtag
def search_tweets_by_hashtag(hashtags, count=10):
    query = ' OR '.join([f'#{hashtag}' for hashtag in hashtags])
    print(f"Buscando tweets con los hashtags: {query}")
    
    try:
        # Buscar tweets recientes con los hashtags
        tweets = api.search_tweets(q=query, count=count, tweet_mode="extended", lang="es")
        
        tweet_data = []
        for tweet in tweets:
            tweet_info = {
                'id': tweet.id_str,
                'text': tweet.full_text,
                'author': tweet.user.screen_name,
                'created_at': str(tweet.created_at)
            }
            tweet_data.append(tweet_info)
            print(f"Tweet capturado: {tweet.full_text}")
            
        return tweet_data
    
    except Exception as e:
        print(f"Error al buscar tweets: {e}")
        return []

# Solicitar hashtags y la cantidad de tweets a capturar
hashtags = input("Introduce los hashtags separados por comas (sin el #): ").split(',')
hashtags = [hashtag.strip() for hashtag in hashtags]
tweet_count = int(input("¿Cuántos tweets deseas capturar? "))

# Buscar los tweets
tweets_data = search_tweets_by_hashtag(hashtags, count=tweet_count)

# Guardar los datos en un archivo JSON
with open('tweets_capturados.json', 'w') as outfile:
    json.dump(tweets_data, outfile, indent=4)

print("Tweets capturados guardados en tweets_capturados.json")
